import asyncio
import logging
from asyncio import Future
from collections import defaultdict
from contextvars import ContextVar
from typing import Any, Coroutine, Dict, List, Optional, Tuple, Type, TypeVar

import attr
import cattr

from scone.common.chanpro import Channel, ChanProHead
from scone.default.utensils.dynamic_dependencies import CanSkipDynamic
from scone.head import Head, Recipe, sshconn
from scone.head.dependency_tracking import (
    CheckOutcomeLabel,
    DepCheckOutcome,
    DependencyCache,
    DependencyTracker,
)
from scone.head.recipe import DepEle, DependencySpec
from scone.sous import utensil_namer
from scone.sous.utensils import Utensil


logger = logging.getLogger(__name__)

current_recipe: ContextVar[Recipe] = ContextVar("current_recipe")

A = TypeVar("A")


class Kitchen:
    def __init__(
        self,
        head: Head,
        dependency_store: DependencyCache,
        notifying_provides: Dict[Recipe, List[DependencySpec]],
    ):
        self._chanproheads: Dict[Tuple[str, str], Future[ChanProHead]] = dict()
        self._dependency_store = dependency_store
        self._dependency_trackers: Dict[Recipe, DependencyTracker] = defaultdict(
            lambda: DependencyTracker(head.pools)
        )
        self.head = head
        self._notifying_provides = notifying_provides
        self.notifications: Dict[DependencySpec, bool] = dict()

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
                    connection_details.get("dangerous_debug_logging", False)
                )
            except Exception:
                logger.error("Failed to open SSH connection", exc_info=True)
                raise

            return ChanProHead(cp, root)

        hostuser = (host, user)
        if hostuser not in self._chanproheads:
            self._chanproheads[hostuser] = asyncio.create_task(new_conn())

        return await self._chanproheads[hostuser]

    async def run_epoch(
        self,
        epoch: List[DepEle],
        depchecks: Dict[Recipe, DepCheckOutcome],
        concurrency_limit_per_host: int = 5,
    ):
        per_host_lists: Dict[str, List[Recipe]] = defaultdict(lambda: [])

        # sort into per-host lists
        for recipe in epoch:
            if isinstance(recipe, Recipe):
                if depchecks[recipe].label != CheckOutcomeLabel.SAFE_TO_SKIP:
                    per_host_lists[recipe.get_host()].append(recipe)

        coros: List[Coroutine] = []

        for host, recipes in per_host_lists.items():
            host_work_pool = HostWorkPool(recipes, depchecks)
            coros.append(host_work_pool.cook_all(self, concurrency_limit_per_host))

        await asyncio.gather(*coros, return_exceptions=False)

    async def start(self, utensil: Utensil) -> Channel:
        utensil_name = utensil_namer(utensil.__class__)
        recipe = current_recipe.get()
        cph = await self.get_chanprohead(recipe.get_host(), recipe.get_user(self.head))

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
        depbook = dependency_tracker.make_depbook()
        if depbook:
            await self._dependency_store.register(recipe, depbook)


@attr.s(auto_attribs=True)
class HostWorkPool:
    jobs: List[Recipe]
    depchecks: Dict[Recipe, DepCheckOutcome]
    next_job: int = 0

    async def cook_all(self, kitchen: Kitchen, concurrency_limit: int):
        num_jobs = len(self.jobs)
        concurrency_limit = min(num_jobs, concurrency_limit)

        async def cooker():
            while self.next_job < num_jobs:
                recipe = self.jobs[self.next_job]
                self.next_job += 1

                current_recipe.set(recipe)
                depcheck = self.depchecks.get(recipe)
                if (
                    depcheck is not None
                    and depcheck.label == CheckOutcomeLabel.CHECK_DYNAMIC
                ):
                    book = depcheck.book
                    assert book is not None
                    can_skip = await kitchen.ut1(
                        CanSkipDynamic(book.dyn_sous_file_hashes)
                    )
                    if can_skip:
                        continue

                await recipe.cook(kitchen)
                # if successful, store dependencies
                await kitchen._store_dependency(recipe)
                nps = kitchen._notifying_provides.get(recipe, None)
                if nps:
                    for depspec in nps:
                        if depspec not in kitchen.notifications:
                            # default to changed if not told otherwise
                            kitchen.notifications[depspec] = True

        await asyncio.gather(
            *[asyncio.create_task(cooker()) for _ in range(concurrency_limit)]
        )
