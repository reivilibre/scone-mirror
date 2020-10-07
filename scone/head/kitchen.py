import asyncio
import logging
from asyncio import Future, Queue
from collections import defaultdict, deque
from contextvars import ContextVar
from typing import Any, Deque, Dict, Optional, Tuple, Type, TypeVar

import cattr
from frozendict import frozendict

from scone.common.chanpro import Channel, ChanProHead
from scone.common.misc import eprint
from scone.head import sshconn
from scone.head.dag import RecipeMeta, RecipeState, Resource, Vertex
from scone.head.dependency_tracking import (
    DependencyBook,
    DependencyCache,
    DependencyTracker,
)
from scone.head.head import Head
from scone.head.recipe import Recipe
from scone.sous import utensil_namer
from scone.sous.utensils import Utensil

logger = logging.getLogger(__name__)

current_recipe: ContextVar[Recipe] = ContextVar("current_recipe")

A = TypeVar("A")


class Preparation:
    def __init__(self, head: Head):
        self.dag = head.dag
        self.head = head
        self._queue: Deque[Tuple[Recipe, RecipeMeta]] = deque()
        self._current_recipe: Optional[Recipe] = None

    def needs(
        self,
        requirement: str,
        identifier: str,
        hard: bool = True,
        sous: Optional[str] = "(self)",
        **extra_identifiers: Any,
    ) -> None:
        assert self._current_recipe is not None

        if sous == "(self)":
            sous = self._current_recipe.recipe_context.sous

        resource = Resource(
            requirement, identifier, sous, frozendict(extra_identifiers)
        )

        self.dag.needs(self._current_recipe, resource, not hard)

    def wants(self, requirement: str, identifier: str, **extra_identifiers: Any):
        return self.needs(requirement, identifier, hard=False, **extra_identifiers)

    def provides(
        self,
        requirement: str,
        identifier: str,
        sous: Optional[str] = "(self)",
        **extra_identifiers: Any,
    ) -> None:
        assert self._current_recipe is not None

        if sous == "(self)":
            sous = self._current_recipe.recipe_context.sous

        resource = Resource(
            requirement, identifier, sous, frozendict(extra_identifiers)
        )

        self.dag.provides(self._current_recipe, resource)

    def after(self, other_recipe: "Recipe"):
        assert self._current_recipe is not None
        self.dag.add_ordering(other_recipe, self._current_recipe)

    def before(self, other_recipe: "Recipe"):
        assert self._current_recipe is not None
        self.dag.add_ordering(self._current_recipe, other_recipe)

    def subrecipe(self, sub: "Recipe"):
        self.dag.add(sub)
        self._queue.append((sub, self.dag.recipe_meta[sub]))

    def prepare_all(self) -> None:
        for recipe in self.dag.vertices:
            if not isinstance(recipe, Recipe):
                continue
            meta = self.dag.recipe_meta[recipe]
            if meta.state != RecipeState.LOADED:
                continue
            self._queue.append((recipe, meta))

        while self._queue:
            recipe, meta = self._queue.popleft()
            self._current_recipe = recipe
            recipe.prepare(self, self.head)
            self._current_recipe = None
            meta.state = RecipeState.PREPARED


class Kitchen:
    def __init__(
        self, head: "Head", dependency_store: DependencyCache,
    ):
        self._chanproheads: Dict[Tuple[str, str], Future[ChanProHead]] = dict()
        self._dependency_store = dependency_store
        self._dependency_trackers: Dict[Recipe, DependencyTracker] = defaultdict(
            lambda: DependencyTracker(DependencyBook(), head.dag)
        )
        self.head = head
        self.last_updated_ats: Dict[Resource, int] = dict()
        self._cookable: Queue[Optional[Vertex]] = Queue()
        self._sleeper_slots: int = 0

    def get_dependency_tracker(self):
        return self._dependency_trackers[current_recipe.get()]

    async def get_chanprohead(self, host: str, user: str) -> ChanProHead:
        async def new_conn():
            connection_details = self.head.souss[host]
            # XXX opt ckey =
            #  os.path.join(self.head.directory, connection_details["clientkey"])

            try:
                cp, root = await sshconn.open_ssh_sous(
                    connection_details["host"],
                    connection_details["user"],
                    None,
                    user,
                    connection_details["souscmd"],
                    connection_details.get("dangerous_debug_logging", False),
                )
            except Exception:
                logger.error("Failed to open SSH connection", exc_info=True)
                raise

            return ChanProHead(cp, root)

        hostuser = (host, user)
        if hostuser not in self._chanproheads:
            self._chanproheads[hostuser] = asyncio.create_task(new_conn())

        return await self._chanproheads[hostuser]

    async def cook_all(self):
        # TODO fridge emitter

        num_workers = 8

        self._sleeper_slots = num_workers - 1

        for vertex in self.head.dag.vertices:
            if isinstance(vertex, Recipe):
                rec_meta = self.head.dag.recipe_meta[vertex]
                if rec_meta.incoming_uncompleted == 0:
                    rec_meta.state = RecipeState.COOKABLE
                    self._cookable.put_nowait(vertex)
                else:
                    rec_meta.state = RecipeState.PENDING
            elif isinstance(vertex, Resource):
                res_meta = self.head.dag.resource_meta[vertex]
                if res_meta.incoming_uncompleted == 0:
                    res_meta.completed = True
                    if res_meta.hard_need:
                        needers = self.head.dag.edges[vertex]
                        needers_str = "".join(f" - {n}\n" for n in needers)
                        raise RuntimeError(
                            f"Hard need 「{vertex}」 not satisfiable."
                            f" Needed by:\n{needers_str}"
                        )
                    self._cookable.put_nowait(vertex)

        workers = []
        for _ in range(num_workers):
            workers.append(self._cooking_worker())

        await asyncio.gather(*workers, return_exceptions=False)

    async def _cooking_worker(self):
        dag = self.head.dag
        while True:
            if self._sleeper_slots <= 0 and self._cookable.empty():
                self._sleeper_slots -= 1
                self._cookable.put_nowait(None)
                break

            self._sleeper_slots -= 1
            try:
                next_job = await self._cookable.get()
            finally:
                self._sleeper_slots += 1

            if next_job is None:
                continue

            if isinstance(next_job, Recipe):
                meta = dag.recipe_meta[next_job]

                # TODO try to deduplicate
                meta.state = RecipeState.BEING_COOKED
                current_recipe.set(next_job)
                eprint(f"cooking {next_job}")
                await next_job.cook(self)
                eprint(f"cooked {next_job}")
                # TODO cook
                # TODO store depbook
                await self._store_dependency(next_job)
                meta.state = RecipeState.COOKED
            elif isinstance(next_job, Resource):
                eprint(f"have {next_job}")
                pass

            for edge in dag.edges[next_job]:
                logger.debug("updating edge: %s → %s", next_job, edge)
                if isinstance(edge, Recipe):
                    rec_meta = dag.recipe_meta[edge]
                    rec_meta.incoming_uncompleted -= 1
                    logger.debug("has %d incoming", rec_meta.incoming_uncompleted)
                    if (
                        rec_meta.incoming_uncompleted == 0
                        and rec_meta.state == RecipeState.PENDING
                    ):
                        rec_meta.state = RecipeState.COOKABLE
                        self._cookable.put_nowait(edge)
                elif isinstance(edge, Resource):
                    res_meta = dag.resource_meta[edge]
                    res_meta.incoming_uncompleted -= 1
                    logger.debug("has %d incoming", res_meta.incoming_uncompleted)
                    if res_meta.incoming_uncompleted == 0 and not res_meta.completed:
                        res_meta.completed = True
                        self._cookable.put_nowait(edge)

    # async def run_epoch(
    #     self,
    #     epoch: List[DepEle],
    #     depchecks: Dict[Recipe, DepCheckOutcome],
    #     concurrency_limit_per_host: int = 5,
    # ):
    #     per_host_lists: Dict[str, List[Recipe]] = defaultdict(lambda: [])
    #
    #     # sort into per-host lists
    #     for recipe in epoch:
    #         if isinstance(recipe, Recipe):
    #             if depchecks[recipe].label != CheckOutcomeLabel.SAFE_TO_SKIP:
    #                 per_host_lists[recipe.get_host()].append(recipe)
    #
    #     coros: List[Coroutine] = []
    #
    #     for host, recipes in per_host_lists.items():
    #         host_work_pool = HostWorkPool(recipes, depchecks)
    #         coros.append(host_work_pool.cook_all(self, concurrency_limit_per_host))
    #
    #     await asyncio.gather(*coros, return_exceptions=False)

    async def start(self, utensil: Utensil) -> Channel:
        utensil_name = utensil_namer(utensil.__class__)
        recipe = current_recipe.get()
        context = recipe.recipe_context
        cph = await self.get_chanprohead(context.sous, context.user)

        # noinspection PyDataclass
        payload = cattr.unstructure(utensil)

        return await cph.start_command_channel(utensil_name, payload)

    ut = start

    async def start_and_consume(self, utensil: Utensil) -> Any:
        channel = await self.start(utensil)
        return await channel.consume()

    ut1 = start_and_consume

    async def start_and_consume_attrs_optional(
        self, utensil: Utensil, attr_class: Type[A]
    ) -> Optional[A]:
        value = await self.start_and_consume(utensil)
        if value is None:
            return None
        return cattr.structure(value, attr_class)

    ut1a = start_and_consume_attrs_optional

    async def start_and_consume_attrs(self, utensil: Utensil, attr_class: Type[A]) -> A:
        value = await self.start_and_consume_attrs_optional(utensil, attr_class)
        if value is None:
            raise ValueError("Received None")
        return value

    ut1areq = start_and_consume_attrs

    async def start_and_wait_close(self, utensil: Utensil) -> Any:
        channel = await self.start(utensil)
        return await channel.wait_close()

    ut0 = start_and_wait_close

    async def _store_dependency(self, recipe: Recipe):
        dependency_tracker = self._dependency_trackers.pop(recipe, None)
        if not dependency_tracker:
            raise KeyError(f"Recipe {recipe} has not been tracked.")
        depbook = dependency_tracker.book
        if depbook:
            await self._dependency_store.register(recipe, depbook)


#
# @attr.s(auto_attribs=True)
# class HostWorkPool:
#     jobs: List[Recipe]
#     depchecks: Dict[Recipe, DepCheckOutcome]
#     next_job: int = 0
#
#     async def cook_all(self, kitchen: Kitchen, concurrency_limit: int):
#         num_jobs = len(self.jobs)
#         concurrency_limit = min(num_jobs, concurrency_limit)
#
#         async def cooker():
#             while self.next_job < num_jobs:
#                 recipe = self.jobs[self.next_job]
#                 self.next_job += 1
#
#                 current_recipe.set(recipe)
#                 depcheck = self.depchecks.get(recipe)
#                 if (
#                     depcheck is not None
#                     and depcheck.label == CheckOutcomeLabel.CHECK_DYNAMIC
#                 ):
#                     book = depcheck.book
#                     assert book is not None
#                     can_skip = await kitchen.ut1(
#                         CanSkipDynamic(book.dyn_sous_file_hashes)
#                     )
#                     if can_skip:
#                         continue
#
#                 await recipe.cook(kitchen)
#                 # if successful, store dependencies
#                 await kitchen._store_dependency(recipe)
#                 nps = kitchen._notifying_provides.get(recipe, None)
#                 if nps:
#                     for depspec in nps:
#                         if depspec not in kitchen.notifications:
#                             # default to changed if not told otherwise
#                             kitchen.notifications[depspec] = True
#
#         await asyncio.gather(
#             *[asyncio.create_task(cooker()) for _ in range(concurrency_limit)]
#         )
