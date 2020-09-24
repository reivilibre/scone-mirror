import asyncio
import json
import logging
import time
from asyncio import Queue
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

import aiosqlite
import attr
import canonicaljson
import cattr
from aiosqlite import Connection

from scone.common.misc import sha256_file
from scone.common.pools import Pools
from scone.head import Head, Recipe, Variables, recipe_name_getter
from scone.head.recipe import DepEle

canonicaljson.set_json_library(json)
logger = logging.getLogger(__name__)

# TODO(security, low): how to prevent passwords being recovered from the
#  paramhashes in a dependency store?

# TODO(correctness, perf): recipes with @src@0 slugs should not be registered
#  to a slug.


def _canonicalise_dict(input: Dict[str, Any]) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    for key, value in input.items():
        if isinstance(value, dict):
            output[key] = _canonicalise_dict(value)
        elif isinstance(value, set):
            new_list = list(value)
            new_list.sort()
            output[key] = new_list
        else:
            output[key] = value
    return output


def hash_dict(value: dict) -> str:
    return sha256(
        canonicaljson.encode_canonical_json(_canonicalise_dict(value))
    ).hexdigest()


def paramhash_recipe(recipe: Recipe) -> str:
    args = getattr(recipe, "_args").copy()
    del args[".source"]
    return hash_dict(args)


@attr.s(auto_attribs=True)
class DependencyBook:
    var_names: List[str]
    var_hash: str
    fridge_hashes: Dict[str, str]
    recipe_revisions: Dict[str, int]
    dyn_sous_file_hashes: Dict[str, str]

    async def can_skip_static(self, head: Head, recipe: Recipe) -> bool:
        from scone.default.steps.fridge_steps import search_in_fridge

        # start with variables
        sous_vars = head.variables[recipe.get_host()]
        var_comp = dict()
        for var_name in self.var_names:
            try:
                var_comp[var_name] = sous_vars.get_dotted(var_name)
            except KeyError:
                return False

        if hash_dict(var_comp) != self.var_hash:
            return False

        # now we have to check files in the fridge
        for fridge_name, expected_hash in self.fridge_hashes.items():
            real_pathstr = search_in_fridge(head, fridge_name)
            if not real_pathstr:
                # vanished locally; that counts as a change
                return False
            real_hash = await asyncio.get_running_loop().run_in_executor(
                head.pools.threaded, sha256_file, real_pathstr
            )
            if real_hash != expected_hash:
                return False

        return True

    def has_dynamic(self) -> bool:
        return len(self.dyn_sous_file_hashes) > 0


class DependencyTracker:
    """
    Tracks the dependencies of a task and then inserts a row as needed.
    """

    def __init__(self, pools: Pools):
        self._vars: Dict[str, Any] = {}
        self._fridge: Dict[str, str] = {}
        self._recipe_revisions: Dict[str, int] = {}
        self._dyn_sous_files: Dict[str, str] = {}
        self._ignored = False
        self._pools = pools

    def ignore(self):
        """
        Call when dependency tracking is not desired (or not advanced enough to
        be useful.)
        """
        self._ignored = True

    async def register_fridge_file(self, fridge_path: str, real_path: str):
        if fridge_path not in self._fridge:
            f_hash = await asyncio.get_running_loop().run_in_executor(
                self._pools.threaded, sha256_file, real_path
            )
            self._fridge[fridge_path] = f_hash

    def register_recipe(self, recipe: Recipe):
        cls = recipe.__class__
        rec_name = recipe_name_getter(cls)
        if not rec_name:
            return
        self._recipe_revisions[rec_name] = getattr(cls, "_REVISION", None)

    def register_variable(self, variable: str, value: Union[dict, str, int]):
        self._vars[variable] = value

    def register_remote_file(self, file: str, file_hash: str):
        self._dyn_sous_files[file] = file_hash

    def make_depbook(self) -> Optional[DependencyBook]:
        if self._ignored:
            return None
        dep_book = DependencyBook(
            list(self._vars.keys()),
            hash_dict(self._vars),
            self._fridge.copy(),
            self._recipe_revisions,
            self._dyn_sous_files,
        )
        return dep_book

    def get_j2_compatible_dep_var_proxies(
        self, variables: Variables
    ) -> Dict[str, "DependencyVarProxy"]:
        result = {}

        for key, vars in variables.toplevel().items():
            result[key] = DependencyVarProxy(self, vars, key + ".")

        return result


class DependencyVarProxy:
    """
    Provides convenient access to variables that also properly tracks
    dependencies.
    """

    def __init__(
        self, dependency_tracker: DependencyTracker, variables: dict, prefix: str = ""
    ):
        self._dvp_dt: DependencyTracker = dependency_tracker
        self._dvp_prefix = prefix
        self._dvp_vars = variables

    def __getattr__(self, key: str):
        fully_qualified_varname = self._dvp_prefix + key
        value = self._dvp_vars.get(key, ...)
        if value is ...:
            raise KeyError(f"Variable does not exist: {fully_qualified_varname}")
        elif isinstance(value, dict):
            return DependencyVarProxy(self._dvp_dt, value, key + ".")
        else:
            self._dvp_dt.register_variable(fully_qualified_varname, value)
            return value


class DependencyCache:
    def __init__(self):
        self.db: Connection = None  # type: ignore
        self.time = int(time.time() * 1000)

    @classmethod
    async def open(cls, path: str) -> "DependencyCache":
        dc = DependencyCache()
        dc.db = await aiosqlite.connect(path)
        await dc.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS dishcache (
                source_file TEXT,
                host TEXT,
                recipe_id TEXT,
                slug TEXT,
                paramhash TEXT,
                dep_book TEXT,
                ts INT,
                PRIMARY KEY (source_file, host, recipe_id, slug, paramhash)
            );
            CREATE INDEX IF NOT EXISTS dishcache_ts ON dishcache (ts);
            """
        )
        await dc.db.commit()
        return dc

    async def sweep_old(self, host: str):
        # TODO(scope creep) allow sweeping only certain source files
        #  so we can do partial execution.
        await self.db.execute(
            """
            DELETE FROM dishcache
                WHERE host = ?
                AND ts < ?
            """,
            (host, self.time),
        )
        await self.db.commit()

    async def inquire(self, recipe: Recipe) -> Optional[Tuple[int, DependencyBook]]:
        paramhash = paramhash_recipe(recipe)
        rows = await self.db.execute_fetchall(
            """
            SELECT rowid, dep_book FROM dishcache
                WHERE source_file = ?
                AND host = ?
                AND recipe_id = ?
                AND paramhash = ?
                AND slug = ?
                LIMIT 1
            """,
            (
                recipe._args[".source"][0],
                recipe.get_host(),
                recipe_name_getter(recipe.__class__),
                paramhash,
                recipe._slug,
            ),
        )
        rows = list(rows)
        if not rows:
            return None

        (rowid, dep_book_json) = rows[0]

        try:
            dep_book = cattr.structure(json.loads(dep_book_json), DependencyBook)
        except Exception:
            logger.error(
                "Failed to structure DependencyBook: %s", dep_book_json, exc_info=True
            )
            raise

        return rowid, dep_book

    async def register(self, recipe: Recipe, dep_book: DependencyBook):
        paramhash = paramhash_recipe(recipe)
        await self.db.execute(
            """
            INSERT INTO dishcache
                (source_file, host, recipe_id, slug, paramhash, dep_book, ts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (source_file, host, recipe_id, paramhash, slug)
                DO UPDATE SET
                    dep_book = excluded.dep_book,
                    ts = excluded.ts
            """,
            (
                recipe._args[".source"][0],
                recipe.get_host(),
                recipe_name_getter(recipe.__class__),
                recipe._slug,
                paramhash,
                canonicaljson.encode_canonical_json(cattr.unstructure(dep_book)),
                self.time,
            ),
        )
        await self.db.commit()

    async def renew(self, rowid: int):
        # TODO(perf): batch up many renews
        await self.db.execute(
            """
            UPDATE dishcache SET ts = ? WHERE rowid = ? LIMIT 1;
            """,
            (self.time, rowid),
        )
        await self.db.commit()


class CheckOutcomeLabel(Enum):
    # Not in dependency cache, so must run.
    NOT_CACHED = 0

    # Dependency cache suggests we must rerun
    MUST_REDO = 1

    # Dependency cache suggests we are fine if dynamic dependencies haven't
    # changed
    CHECK_DYNAMIC = 2

    # Dependency cache says we can skip; there are no dynamic dependencies
    SAFE_TO_SKIP = 3


DepCheckOutcome = NamedTuple(
    "DepCheckOutcome",
    (("label", CheckOutcomeLabel), ("book", Optional[DependencyBook])),
)


async def run_dep_checks(
    head: Head, dep_cache: DependencyCache, order: List[List[DepEle]]
) -> Dict[Recipe, DepCheckOutcome]:
    queue: Queue[Optional[Recipe]] = Queue(32)
    outcomes = {}

    async def consumer():
        while True:
            recipe = await queue.get()
            if not recipe:
                break
            t = await dep_cache.inquire(recipe)
            if t:
                # we need to check if dependencies have changed…
                rowid, dep_book = t
                if await dep_book.can_skip_static(head, recipe):
                    # we will renew either way
                    await dep_cache.renew(rowid)
                    if dep_book.has_dynamic():
                        # has dynamic dependencies
                        outcomes[recipe] = DepCheckOutcome(
                            CheckOutcomeLabel.CHECK_DYNAMIC, dep_book
                        )
                    else:
                        # can skip!
                        outcomes[recipe] = DepCheckOutcome(
                            CheckOutcomeLabel.SAFE_TO_SKIP, None
                        )
                else:
                    outcomes[recipe] = DepCheckOutcome(
                        CheckOutcomeLabel.MUST_REDO, None
                    )
            else:
                outcomes[recipe] = DepCheckOutcome(CheckOutcomeLabel.NOT_CACHED, None)
            queue.task_done()

    async def producer():
        for course in order:
            for recipe in course:
                if isinstance(recipe, Recipe):
                    await queue.put(recipe)
        await queue.join()
        for worker in consumers:
            await queue.put(None)

    consumers = [asyncio.create_task(consumer()) for _ in range(8)]
    await asyncio.gather(*consumers, producer(), return_exceptions=False)

    return outcomes
