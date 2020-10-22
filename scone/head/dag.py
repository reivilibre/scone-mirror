from collections import defaultdict
from enum import Enum
from typing import Dict, Optional, Set, Union

import attr
from frozendict import frozendict

from scone.head.recipe import Recipe


class RecipeState(Enum):
    # Just loaded from menu, or otherwise created
    LOADED = 0

    # Has been prepared — we know its dependencies for this run
    PREPARED = 1

    # This recipe needs to be cooked, but may be blocked by dependencies
    PENDING = 2

    # This recipe is not blocked by any further
    COOKABLE = 3

    # This recipe is being cooked
    BEING_COOKED = 4

    # This recipe has been cooked!
    COOKED = 5

    # This recipe has not been cooked because it didn't need to be.
    SKIPPED = 10

    # This recipe failed.
    FAILED = -1

    @staticmethod
    def is_completed(state):
        return state in (RecipeState.COOKED, RecipeState.SKIPPED)


@attr.s(auto_attribs=True)
class RecipeMeta:
    """
    State of the recipe.
    """

    state: RecipeState = RecipeState.LOADED

    """
    Uncompleted incoming edge count.
    """
    incoming_uncompleted: int = 0


@attr.s(auto_attribs=True, frozen=True)
class Resource:
    """
    Resource kind.
    """

    kind: str

    """
    Resource ID
    """
    id: str

    """
    Resource sous, or None if it's on the head
    """
    sous: Optional[str]

    """
    Optional dict of extra parameters needed to disambiguate the resource,
    though should only be used where necessary and sensible to do so.
    """
    # extra_params: Optional[frozendict[str, str]] = None
    extra_params: Optional[frozendict] = None

    def __str__(self) -> str:
        extra_str = "" if not self.extra_params else f" {self.extra_params!r}"
        sous_str = "" if not self.sous else f" on {self.sous}"
        return f"{self.kind}({self.id}){extra_str}{sous_str}"


@attr.s(auto_attribs=True)
class ResourceMeta:
    """
    Whether the resource is completed or not.
    A resource becomes completed when all its incoming edges are completed,
    or it has no incoming edges and is not a hard need.
    """

    completed: bool = False

    """
    Uncompleted incoming edge count.
    """
    incoming_uncompleted: int = 0

    """
    Whether the resource is considered a hard need.
    A resource is a hard need when we cannot proceed without something
    providing it.
    """
    hard_need: bool = False


Vertex = Union["Recipe", Resource]


class RecipeDag:
    def __init__(self):
        self.vertices: Set[Vertex] = set()
        # edges go from A -> B where B needs A to run.
        self.edges: Dict[Vertex, Set[Vertex]] = defaultdict(set)
        self.reverse_edges: Dict[Vertex, Set[Vertex]] = defaultdict(set)
        self.recipe_meta: Dict[Recipe, RecipeMeta] = dict()
        self.resource_meta: Dict[Resource, ResourceMeta] = dict()

        self.resource_time: Dict[Resource, int] = dict()

    def add(self, vertex: Vertex):
        self.vertices.add(vertex)
        if isinstance(vertex, Recipe):
            self.recipe_meta[vertex] = RecipeMeta()
        elif isinstance(vertex, Resource):
            self.resource_meta[vertex] = ResourceMeta()

    def needs(
        self, needer: "Recipe", resource: Resource, soft_wants: bool = False
    ) -> None:
        if needer not in self.vertices:
            raise ValueError(f"Needer {needer} not in vertices!")

        if resource not in self.vertices:
            self.add(resource)

        if needer in self.edges[resource]:
            return

        self.edges[resource].add(needer)
        self.reverse_edges[needer].add(resource)

        needer_meta = self.recipe_meta[needer]
        resource_meta = self.resource_meta[resource]

        if not soft_wants:
            resource_meta.hard_need = True

        if not resource_meta.completed:
            needer_meta.incoming_uncompleted += 1

    def provides(self, provider: "Recipe", resource: Resource) -> None:
        if provider not in self.vertices:
            raise ValueError(f"Provider {provider} not in vertices!")

        if resource not in self.vertices:
            self.add(resource)

        if resource in self.edges[provider]:
            return

        self.edges[provider].add(resource)
        self.reverse_edges[resource].add(provider)

        provider_meta = self.recipe_meta[provider]
        resource_meta = self.resource_meta[resource]

        if not RecipeState.is_completed(provider_meta.state):
            resource_meta.incoming_uncompleted += 1
            resource_meta.completed = False
        else:
            if resource_meta.incoming_uncompleted == 0:
                resource_meta.completed = True

    def add_ordering(self, before: "Recipe", after: "Recipe") -> None:
        if before not in self.vertices:
            raise ValueError(f"Before {before} not in vertices!")

        if after not in self.vertices:
            raise ValueError(f"After {after} not in vertices!")

        after_meta = self.recipe_meta[after]
        before_meta = self.recipe_meta[before]

        if after in self.edges[before]:
            return

        self.edges[before].add(after)
        self.reverse_edges[after].add(before)

        if not RecipeState.is_completed(before_meta.state):
            after_meta.incoming_uncompleted += 1
            # TODO if after_meta.state ==
        # TODO else ...
