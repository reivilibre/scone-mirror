from pathlib import PurePath
from typing import List, Optional, Union

from scone.default.utensils.basic_utensils import SimpleExec
from scone.head.exceptions import CookingError
from scone.head.kitchen import Kitchen, current_recipe
from scone.head.recipe import Recipe


class ExecutionFailure(CookingError):
    """
    A command failed.
    """

    def __init__(
        self,
        args: List[str],
        working_dir: str,
        sous: str,
        user: str,
        result: SimpleExec.Result,
    ):
        stderr = result.stderr.decode().replace("\n", "\n    ")

        message = (
            f"Command failed on {sous} (user {user}) in {working_dir}.\n"
            f"The command was: {args}\n"
            f"Stderr was:\n    {stderr}"
        )

        super().__init__(message)


async def exec_no_fails(
    kitchen: Kitchen, args: List[str], working_dir: Union[str, PurePath]
) -> SimpleExec.Result:
    if not isinstance(working_dir, str):
        working_dir = str(working_dir)

    result = await kitchen.start_and_consume_attrs(
        SimpleExec(args, working_dir), SimpleExec.Result
    )

    if result.exit_code != 0:
        recipe: Optional[Recipe] = current_recipe.get(None)  # type: ignore
        if recipe:
            raise ExecutionFailure(
                args,
                working_dir,
                recipe.recipe_context.sous,
                recipe.recipe_context.user,
                result,
            )
        else:
            raise ExecutionFailure(args, working_dir, "???", "???", result)

    return result
