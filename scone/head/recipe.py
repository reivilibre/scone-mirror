import typing
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import toposort

if typing.TYPE_CHECKING:
    from scone.head import Head
    from scone.head.kitchen import Kitchen

DependencySpec = Tuple[str, str, frozenset]
DepEle = Union["Recipe", DependencySpec]


class Preparation:
    """
    Preparation on a single host.
    This is done before we start deduplicating (could this be improved?).
    """

    def __init__(self, recipes: List["Recipe"]):
        self._to_process = recipes.copy()
        self.recipes = recipes
        self._dependencies: Dict[DepEle, Set[DepEle]] = {}
        self._recipe_now: Optional[Recipe] = None
        self._hard_needs: Set[DependencySpec] = set()
        self.notifying_provides: Dict[Recipe, List[DependencySpec]] = defaultdict(
            lambda: []
        )

    def _make_depspec_tuple(
        self, requirement: str, identifier: str, **extra_identifiers: Any
    ) -> DependencySpec:
        if "host" not in extra_identifiers:
            assert self._recipe_now is not None
            extra_identifiers["host"] = self._recipe_now.get_host()
        return requirement, identifier, frozenset(extra_identifiers.items())

    def needs(
        self,
        requirement: str,
        identifier: str,
        hard: bool = False,
        **extra_identifiers: Any,
    ) -> None:
        assert self._recipe_now is not None
        if self._recipe_now not in self._dependencies:
            self._dependencies[self._recipe_now] = set()
        depspec_tuple = self._make_depspec_tuple(
            requirement, identifier, **extra_identifiers
        )
        self._dependencies[self._recipe_now].add(depspec_tuple)
        if hard:
            self._hard_needs.add(depspec_tuple)

    def provides(
        self,
        requirement: str,
        identifier: str,
        notifying: bool = False,
        **extra_identifiers: Any,
    ) -> None:
        assert self._recipe_now is not None
        depspec_tuple = self._make_depspec_tuple(
            requirement, identifier, **extra_identifiers
        )
        if depspec_tuple not in self._dependencies:
            self._dependencies[depspec_tuple] = set()
        self._dependencies[depspec_tuple].add(self._recipe_now)
        if notifying:
            self.notifying_provides[self._recipe_now].append(depspec_tuple)

    def subrecipe(self, recipe: "Recipe"):
        assert self._recipe_now is not None
        self._to_process.append(recipe)
        self.recipes.append(recipe)
        args = getattr(recipe, "_args")
        if ".source" not in args:
            file, key, slug = getattr(self._recipe_now, "_args")[".source"]
            args[".source"] = (file, key + "-sub", slug)

    def prepare(self, head: "Head") -> List[List]:
        while self._to_process:
            next_recipe = self._to_process.pop()
            self._recipe_now = next_recipe
            next_recipe.prepare(self, head)

        for hard_need in self._hard_needs:
            if hard_need not in self._dependencies:
                raise ValueError(f"Hard need not satisfied (no entry): {hard_need}")
            if not self._dependencies[hard_need]:
                raise ValueError(f"Hard need not satisfied (empty): {hard_need}")

        self._dependencies[self._make_depspec_tuple(".internal", "completed")] = set(
            self.recipes
        )
        return list(toposort.toposort(self._dependencies))


def recipe_name_getter(c: typing.Type["Recipe"]) -> Optional[str]:
    if hasattr(c, "_NAME"):
        return c._NAME  # type: ignore
    return None


class Recipe:
    def __init__(self, host: str, slug: str, args: dict, head: "Head"):
        self._host = host
        self._slug = slug
        self._args = args

    def get_host(self):
        return self._host

    def get_tweak(self, name: str, default: Any) -> Any:
        dotname = f".{name}"
        if dotname in self._args:
            return self._args[dotname]
        elif ".m" in self._args and name in self._args[".m"]:
            return self._args[".m"][name]
        else:
            return default

    def get_user(self, head: "Head") -> str:
        user = self.get_tweak("user", head.souss[self._host]["user"])
        assert isinstance(user, str)
        return user

    @classmethod
    def from_menu(cls, host: str, slug: str, args: dict, head: "Head") -> "Recipe":
        return cls(host, slug, args, head)

    def prepare(self, preparation: Preparation, head: "Head") -> None:
        preparation.needs("os-user", self.get_user(head))

        # TODO(feature) allow merging per-task and per-menu tweaks
        # TODO(feature) allow need/provide custom things, not just user-units

        afters = self.get_tweak("needs", None)
        if afters:
            for after in afters:
                if isinstance(after, list) and len(after) == 2:
                    # allow requesting custom needs
                    preparation.needs(after[0], after[1])
                    continue
                if not isinstance(after, str):
                    raise ValueError("needs tweak should be list of strings or pairs.")
                preparation.needs("user-unit", after)

        befores = self.get_tweak("provides", None)
        if befores:
            if isinstance(befores, str):
                preparation.provides("user-unit", befores)
            else:
                for before in befores:
                    if not isinstance(before, str):
                        raise ValueError("provides tweak should be list of strings.")
                    preparation.provides("user-unit", before)

    async def cook(self, kitchen: "Kitchen") -> None:
        raise NotImplementedError

    def __str__(self):
        cls = self.__class__
        if hasattr(cls, "RECIPE_NAME"):
            return (
                f"{cls.RECIPE_NAME}({cls.__name__}) {self._slug} "  # type: ignore
                f"on {self._host} ({self._args})"
            )
        else:
            return f"{cls.__name__} {self._slug} on {self._host} ({self._args})"
