import json
import logging
import time
from copy import deepcopy
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

import aiosqlite
import attr
import canonicaljson
import cattr
from aiosqlite import Connection

from scone.head.dag import Resource
from scone.head.recipe import recipe_name_getter
from scone.head.variables import Variables

if TYPE_CHECKING:
    from scone.head.dag import RecipeDag
    from scone.head.recipe import Recipe

canonicaljson.set_json_library(json)
logger = logging.getLogger(__name__)

# TODO(security, low): how to prevent passwords being recovered from the
#  paramhashes in a dependency store?


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


def paramhash_recipe(recipe: "Recipe") -> str:
    return hash_dict(
        {
            "args": recipe.arguments,
            "sous": recipe.recipe_context.sous,
            "user": recipe.recipe_context.user,
        }
    )


@attr.s(auto_attribs=True)
class DependencyBook:
    provided: Dict[Resource, int] = dict()
    watching: Dict[Resource, int] = dict()
    last_changed: int = 0
    cache_data: Dict[str, Any] = dict()
    ignored: bool = False

    # TODO(performance, feature): track more in-depth details, perhaps as a
    #     per-resource cache thing, so that we can track the info needed to know
    #     if it changed...?

    def _unstructure(self) -> dict:
        return {
            "provided": cattr.unstructure(tuple(self.provided.items())),
            "watching": cattr.unstructure(tuple(self.watching.items())),
            "last_changed": self.last_changed,
            "cache_data": self.cache_data,
            "ignored": self.ignored,
        }

    @staticmethod
    def _structure(dictionary: dict) -> "DependencyBook":
        provided = {cattr.structure(k, Resource): v for k, v in dictionary["provided"]}
        watching = {cattr.structure(k, Resource): v for k, v in dictionary["watching"]}

        return DependencyBook(
            provided=provided,
            watching=watching,
            last_changed=dictionary["last_changed"],
            cache_data=dictionary["cache_data"],
            ignored=dictionary["ignored"],
        )


cattr.global_converter.register_unstructure_hook(
    DependencyBook, DependencyBook._unstructure
)
cattr.global_converter.register_structure_hook(
    DependencyBook, DependencyBook._structure
)


class DependencyTracker:
    def __init__(self, book: DependencyBook, dag: "RecipeDag", recipe: "Recipe"):
        self.book: DependencyBook = book
        self._dag: "RecipeDag" = dag
        self._recipe: "Recipe" = recipe
        self._time: int = int(time.time() * 1000)

    def watch(self, resource: Resource) -> None:
        # XXX self.book.watching[resource] = self._dag.resource_time[resource]
        self.book.watching[resource] = -42

    def provide(self, resource: Resource, time: Optional[int] = None) -> None:
        if time is None:
            time = self._time
        self._dag.resource_time[resource] = time

    def ignore(self) -> None:
        self.book.ignored = True

    def register_variable(self, variable: str, value: Union[dict, str, int]):
        # self._vars[variable] = value
        # TODO(implement)
        logger.critical("not implemented: register var %s", variable)

    def register_fridge_file(self, desugared_path: str):
        # TODO this is not complete
        fridge_res = Resource("fridge", desugared_path, None)
        self.watch(fridge_res)

    def register_remote_file(self, path: str, sous: Optional[str] = None):
        sous = sous or self._recipe.recipe_context.sous
        # TODO this is not complete
        file_res = Resource("file", path, sous=sous)
        self.watch(file_res)

    def get_j2_compatible_dep_var_proxies(
        self, variables: Variables
    ) -> Dict[str, "DependencyVarProxy"]:
        result = {}

        for key in variables.toplevel():
            result[key] = DependencyVarProxy(key, variables, self)

        return result


class DependencyVarProxy:
    def __init__(
        self,
        current_path_prefix: Optional[str],
        vars: Variables,
        tracker: DependencyTracker,
    ):
        self._current_path_prefix = current_path_prefix
        self._vars = vars
        self._tracker = tracker

    def raw_(self) -> Dict[str, Any]:
        if not self._current_path_prefix:
            raw_dict = self._vars.toplevel()
        else:
            raw_dict = self._vars.get_dotted(self._current_path_prefix)
        self._tracker.register_variable(self._current_path_prefix or "", raw_dict)
        return deepcopy(raw_dict)

    def __getattr__(self, name: str) -> Union["DependencyVarProxy", Any]:
        if not self._current_path_prefix:
            dotted_path = name
        else:
            dotted_path = f"{self._current_path_prefix}.{name}"
        raw_value = self._vars.get_dotted(dotted_path)

        if isinstance(raw_value, dict):
            return DependencyVarProxy(dotted_path, self._vars, self._tracker)
        else:
            self._tracker.register_variable(dotted_path, raw_value)
            return raw_value


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
                -- source_file TEXT,
                recipe_kind TEXT,
                paramhash TEXT,
                dep_book TEXT,
                ts INT,
                PRIMARY KEY (recipe_kind, paramhash)
            );
            -- CREATE INDEX IF NOT EXISTS dishcache_ts ON dishcache (ts);
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

    async def inquire(self, recipe: "Recipe") -> Optional[Tuple[int, DependencyBook]]:
        paramhash = paramhash_recipe(recipe)
        rows = await self.db.execute_fetchall(
            """
            SELECT rowid, dep_book FROM dishcache
                WHERE recipe_kind = ?
                AND paramhash = ?
                LIMIT 1
            """,
            (recipe_name_getter(recipe.__class__), paramhash,),
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

    async def register(self, recipe: "Recipe", dep_book: DependencyBook):
        paramhash = paramhash_recipe(recipe)
        await self.db.execute(
            """
            INSERT INTO dishcache
                (recipe_kind, paramhash, dep_book, ts)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (recipe_kind, paramhash)
                DO UPDATE SET
                    dep_book = excluded.dep_book,
                    ts = excluded.ts
            """,
            (
                recipe_name_getter(recipe.__class__),
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
