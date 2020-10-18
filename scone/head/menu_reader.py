import logging
import os
import typing
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import attr
import textx

from scone.head.dag import RecipeDag, Resource
from scone.head.recipe import RecipeContext
from scone.head.variables import Variables

if typing.TYPE_CHECKING:
    from scone.head.head import Head
    from scone.head.recipe import Recipe


def _load_grammar():
    grammar_file_path = Path(Path(__file__).parent, "grammar", "scoml.tx")
    return textx.metamodel_from_file(grammar_file_path)


scoml_grammar = _load_grammar()
scoml_classes = scoml_grammar.namespaces["scoml"]

logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True)
class ForDirective:
    """
    For loop_variable in collection
    """

    # The name of the variable that should be taken on by the iteration
    loop_variable: str

    # List of literals or str for a variable (by name)
    collection: Union[str, List[Any]]


@attr.s(auto_attribs=True)
class RecipeEdgeDirective:
    # "after" or "before"
    kind: str

    recipe_id: str


@attr.s(auto_attribs=True)
class ResourceEdgeDirective:
    # "needs", "wants" or "provides"
    kind: str

    resource: Resource


@attr.s(auto_attribs=True)
class MenuBlock:
    id: Optional[None]

    human: str

    contents: List[Union["MenuBlock", "MenuRecipe"]]

    parent: Optional["MenuBlock"]

    user_directive: Optional[str] = None
    sous_directive: Optional[str] = None
    for_directives: List[ForDirective] = []
    import_directives: List[str] = []
    recipe_edges: List[RecipeEdgeDirective] = []
    resource_edges: List[ResourceEdgeDirective] = []


@attr.s(auto_attribs=True, eq=False)
class MenuRecipe:
    kind: str

    id: Optional[str]

    human: str

    arguments: Dict[str, Any]

    parent: MenuBlock

    user_directive: Optional[str] = None
    sous_directive: Optional[str] = None
    for_directives: List[ForDirective] = []
    recipe_edges: List[RecipeEdgeDirective] = []
    resource_edges: List[ResourceEdgeDirective] = []


def convert_textx_value(txvalue) -> Any:
    if isinstance(txvalue, scoml_classes["NaturalList"]):
        return [convert_textx_value(element) for element in txvalue.elements]
    elif (
        isinstance(txvalue, scoml_classes["QuotedString"])
        or isinstance(txvalue, scoml_classes["UnquotedString"])
        or isinstance(txvalue, scoml_classes["Integer"])
        or isinstance(txvalue, scoml_classes["Boolean"])
    ):
        return txvalue.value
    elif isinstance(txvalue, scoml_classes["BracketList"]):
        return [convert_textx_value(item) for item in txvalue.items]
    elif isinstance(txvalue, scoml_classes["BraceDict"]):
        result = dict()
        for pair in txvalue.pairs:
            result[convert_textx_value(pair.key)] = convert_textx_value(pair.value)
    else:
        raise ValueError(f"Unknown SCOML value: {txvalue}")


def convert_textx_recipe(txrecipe_or_subblock, parent: Optional[MenuBlock]):
    if isinstance(txrecipe_or_subblock, scoml_classes["SubBlock"]):
        txsubblock = txrecipe_or_subblock
        menu_block = convert_textx_block(txsubblock.block, parent)
        menu_block.id = txsubblock.unique_id
        menu_block.human = txsubblock.human.strip()

        return menu_block
    elif isinstance(txrecipe_or_subblock, scoml_classes["Recipe"]):
        assert parent is not None
        txrecipe = txrecipe_or_subblock
        args = dict()

        for arg in txrecipe.args:
            args[arg.name] = convert_textx_value(arg.value)
        recipe = MenuRecipe(
            txrecipe.kind, txrecipe.unique_id, txrecipe.human.strip(), args, parent
        )

        for directive in txrecipe.directives:
            if isinstance(directive, scoml_classes["UserDirective"]):
                recipe.user_directive = directive.user
            elif isinstance(directive, scoml_classes["SousDirective"]):
                recipe.user_directive = directive.sous
            else:
                raise ValueError(f"Unknown directive {directive}")

        return recipe
    else:
        raise ValueError("Neither Recipe nor SubBlock: " + str(txrecipe_or_subblock))


def convert_textx_resource(txresource) -> Resource:
    extra_params = None
    if txresource.extra_params is not None:
        extra_params = convert_textx_value(txresource.extra_params)

    sous: Optional[str] = "(self)"  # XXX docstring to warn about this
    if txresource.sous:
        if txresource.sous == "head":
            sous = None
        else:
            sous = txresource.sous

    return Resource(txresource.type, txresource.primary, sous, extra_params)


def convert_textx_block(txblock, parent: Optional[MenuBlock]) -> MenuBlock:
    recipes: List[Union[MenuRecipe, MenuBlock]] = []
    block = MenuBlock(None, "", recipes, parent)

    for recipe in txblock.recipes:
        recipes.append(convert_textx_recipe(recipe, block))

    for directive in txblock.directives:
        if isinstance(directive, scoml_classes["UserDirective"]):
            # TODO(expectation): error if multiple user directives
            block.user_directive = directive.user
        elif isinstance(directive, scoml_classes["SousDirective"]):
            block.sous_directive = directive.sous
        elif isinstance(directive, scoml_classes["ForDirective"]):
            block.for_directives.append(
                ForDirective(
                    directive.loop_variable,
                    directive.collection or convert_textx_value(directive.list),
                )
            )
        elif isinstance(directive, scoml_classes["ImportDirective"]):
            block.import_directives.append(directive.importee)
        elif isinstance(directive, scoml_classes["ResourceEdgeDirective"]):
            block.resource_edges.append(
                ResourceEdgeDirective(
                    directive.kind, convert_textx_resource(directive.resource)
                )
            )
        elif isinstance(directive, scoml_classes["RecipeEdgeDirective"]):
            block.recipe_edges.append(RecipeEdgeDirective(directive.kind, directive.id))
        else:
            raise ValueError(f"Unknown directive {directive}")

    return block


SousName = str
ForLoopIndices = Tuple[int, ...]
SingleRecipeInvocationKey = Tuple[SousName, ForLoopIndices]


class MenuLoader:
    def __init__(self, menu_dir: Path, head: "Head"):
        self._menu_dir: Path = menu_dir
        self._units: Dict[str, MenuBlock] = dict()
        self._recipes: Dict[
            MenuRecipe, Dict[SingleRecipeInvocationKey, Recipe]
        ] = defaultdict(dict)
        self._dag: RecipeDag = head.dag
        self._head = head

    @staticmethod
    def _load_menu_unit(full_path: Path, relative: str) -> MenuBlock:
        model = scoml_grammar.model_from_file(full_path)
        return convert_textx_block(model, None)

    def load(self, unit_name: str):
        if unit_name in self._units:
            return

        full_path = Path(self._menu_dir, unit_name + ".scoml")
        menu_block = self._load_menu_unit(full_path, unit_name)
        self._units[unit_name] = menu_block
        for unit in menu_block.import_directives:
            self.load(unit)

    def resolve_ref(
        self, referrer: Union[MenuBlock, MenuRecipe], reference: str
    ) -> Optional[Union[MenuBlock, MenuRecipe]]:
        """
        Resolves a recipe or block reference
        :param referrer: recipe or block making the reference that needs to be resolved
        :param reference: string reference that needs to be resolved
        :return: If found, the menu block or recipe that was referenced.
        """
        # TODO(feature): need to think about scoping rules and then figure
        #  this one out
        return None

    def _get_first_common_ancestor(
        self, one: Union[MenuBlock, MenuRecipe], other: Union[MenuBlock, MenuRecipe]
    ) -> Optional[MenuBlock]:
        ancestors_of_a = set()

        a: Optional[Union[MenuBlock, MenuRecipe]] = one
        b: Optional[Union[MenuBlock, MenuRecipe]] = other

        while a:
            ancestors_of_a.add(a)
            a = a.parent

        while b:
            if b in ancestors_of_a:
                assert isinstance(b, MenuBlock)
                return b
            b = b.parent

        return None

    def get_related_instances(
        self,
        sous: str,
        referrer_indices: Tuple[int, ...],
        referrer: Union[MenuBlock, MenuRecipe],
        menu_recipe: MenuRecipe,
    ) -> List["Recipe"]:
        result = []

        first_common_ancestor = self._get_first_common_ancestor(referrer, menu_recipe)

        a: Union[MenuBlock, MenuRecipe] = referrer
        strip = 0
        while a != first_common_ancestor:
            strip += len(a.for_directives)
            parent = a.parent
            assert parent is not None
            a = parent

        a = menu_recipe
        extra = 0
        while a != first_common_ancestor:
            extra += len(a.for_directives)
            parent = a.parent
            assert parent is not None
            a = parent

        for (instance_sous, indices), recipe in self._recipes[menu_recipe].items():
            if sous != instance_sous:
                continue
            if len(referrer_indices) - strip + extra == len(indices):
                if referrer_indices[:-strip] == indices[:-extra]:
                    result.append(recipe)
            else:
                logger.warning(
                    "Mismatch in indices length %r - %d + %d ~/~ %r",
                    referrer_indices,
                    strip,
                    extra,
                    indices,
                )

        return result

    def dagify_recipe(
        self,
        recipe: MenuRecipe,
        hierarchical_source: str,
        fors: Tuple[ForDirective, ...],
        applicable_souss: Iterable[str],
        applicable_user: Optional[str],
    ):
        recipe_class = self._head.recipe_loader.get_class(recipe.kind)

        fors = fors + tuple(recipe.for_directives)

        if recipe.user_directive:
            applicable_user = recipe.user_directive

        if recipe.sous_directive:
            applicable_souss = self._head.get_souss_for_hostspec(recipe.sous_directive)

        for sous in applicable_souss:
            if not applicable_user:
                applicable_user = self._head.souss[sous]["user"]
                assert applicable_user is not None

            sous_vars = self._head.variables[sous]
            for _vars, for_indices in self._for_apply(fors, sous_vars, tuple()):
                context = RecipeContext(
                    sous=sous,
                    user=applicable_user,
                    slug=recipe.id,
                    hierarchical_source=hierarchical_source,  # XXX
                    human=recipe.human,
                )
                args = recipe.arguments  # noqa
                # XXX sub in vars
                instance: Recipe = recipe_class.new(
                    context, recipe.arguments, self._head
                )
                self._recipes[recipe][(sous, for_indices)] = instance
                self._dag.add(instance)

    def dagify_block(
        self,
        block: MenuBlock,
        hierarchical_source: str,
        fors: Tuple[ForDirective, ...],
        applicable_souss: Iterable[str],
        applicable_user: Optional[str],
    ):
        fors = fors + tuple(block.for_directives)

        if block.user_directive:
            applicable_user = block.user_directive

        if block.sous_directive:
            applicable_souss = self._head.get_souss_for_hostspec(block.sous_directive)

        for content in block.contents:
            if isinstance(content, MenuBlock):
                block_name = content.id or "?"
                self.dagify_block(
                    content,
                    f"{hierarchical_source}.{block_name}",
                    fors,
                    applicable_souss,
                    applicable_user,
                )
            elif isinstance(content, MenuRecipe):
                self.dagify_recipe(
                    content,
                    hierarchical_source,
                    fors,
                    applicable_souss,
                    applicable_user,
                )
            else:
                raise ValueError(f"{content}?")

    def postdagify_recipe(
        self,
        recipe: MenuRecipe,
        fors: Tuple[ForDirective, ...],
        applicable_souss: Iterable[str],
    ):
        # add fors
        fors = fors + tuple(recipe.for_directives)

        if recipe.sous_directive:
            applicable_souss = self._head.get_souss_for_hostspec(recipe.sous_directive)

        for sous in applicable_souss:
            sous_vars = self._head.variables[sous]
            for _vars, for_indices in self._for_apply(fors, sous_vars, tuple()):
                instance = self._recipes[recipe][(sous, for_indices)]  # noqa

                # XXX apply specific edges here including those from parent

    def postdagify_block(
        self,
        block: MenuBlock,
        fors: Tuple[ForDirective, ...],
        applicable_souss: Iterable[str],
    ):
        # XXX pass down specific edges here

        fors = fors + tuple(block.for_directives)

        if block.sous_directive:
            applicable_souss = self._head.get_souss_for_hostspec(block.sous_directive)

        for content in block.contents:
            if isinstance(content, MenuBlock):
                self.postdagify_block(content, fors, applicable_souss)
            elif isinstance(content, MenuRecipe):
                self.postdagify_recipe(content, fors, applicable_souss)
            else:
                raise ValueError(f"{content}?")

    def dagify_all(self):
        for name, unit in self._units.items():
            self.dagify_block(
                unit, name, tuple(), self._head.get_souss_for_hostspec("all"), None
            )
        for _name, unit in self._units.items():
            self.postdagify_block(
                unit, tuple(), self._head.get_souss_for_hostspec("all")
            )

    def _for_apply(
        self, fors: Tuple[ForDirective, ...], vars: "Variables", accum: Tuple[int, ...]
    ) -> Iterable[Tuple["Variables", Tuple[int, ...]]]:
        if not fors:
            yield vars, accum
            return

        head = fors[0]
        tail = fors[1:]

        to_iter = head.collection
        if isinstance(to_iter, str):
            to_iter = vars.get_dotted(to_iter)

        if not isinstance(to_iter, list):
            raise ValueError(f"to_iter = {to_iter!r} not a list")

        for idx, item in enumerate(to_iter):
            new_vars = Variables(vars)
            new_vars.set_dotted(head.loop_variable, item)
            yield from self._for_apply(tail, new_vars, accum + (idx,))

    def load_menus_in_dir(self) -> RecipeDag:
        dag = RecipeDag()

        for root, dirs, files in os.walk(self._menu_dir):
            for file in files:
                if not file.endswith(".scoml"):
                    continue
                # full_path = Path(root, file)
                # load this as a menu file
                pieces = file.split(".")
                assert len(pieces) == 2
                self.load(pieces[0])

        return dag
