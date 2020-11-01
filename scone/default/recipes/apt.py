import asyncio
import logging
from typing import Dict, List, Set, Tuple

from scone.default.utensils.basic_utensils import SimpleExec
from scone.head.head import Head
from scone.head.kitchen import Kitchen, Preparation
from scone.head.recipe import Recipe, RecipeContext
from scone.head.utils import check_type

logger = logging.getLogger(__name__)


# class AptInstallInternal(Recipe):
#     """
#     Actually installs the packages; does it in a single batch for efficiency!
#     """
#
#     _NAME = "apt-install.internal"
#
#     # TODO(extension, low): expand this into apt-install-now if we need
#     #     the flexibility
#
#     def __init__(self, recipe_context: RecipeContext, args: dict, head):
#         super().__init__(recipe_context, args, head)
#
#         self.packages: Set[str] = set()
#
#         args["packages"] = self.packages
#         args[".source"] = ("@virtual", "apt-install-internal", "the one true AII")
#
#     def get_user(self, head: "Head") -> str:
#         return "root"
#
#     def prepare(self, preparation: Preparation, head: "Head") -> None:
#         super().prepare(preparation, head)
#         preparation.needs("apt-stage", "internal-install-packages")
#         preparation.needs("apt-stage", "repositories-declared")
#         preparation.provides("apt-stage", "packages-installed")
#
#     async def _apt_command(self, kitchen: Kitchen, args: List[str]) -> SimpleExec.Result:
#         # lock_path = "/var/lib/apt/lists/lock"
#         lock_path = "/var/lib/dpkg/lock"
#
#         retries = 3
#
#         while retries > 0:
#             result = await kitchen.ut1areq(
#                 SimpleExec(args, "/"), SimpleExec.Result
#             )
#
#             if result.exit_code == 0 or b"/lock" not in result.stderr:
#                 return result
#
#             logger.
#
#             retries -= 1
#
#             # /lock seen in stderr, probably a locking issue...
#             lock_check = await kitchen.ut1areq(SimpleExec(
#                 ["fuser", lock_path],
#                 "/"
#             ), SimpleExec.Result)
#
#             if lock_check.exit_code != 0:
#                 # non-zero code means the file is not being accessed;
#                 # use up a retry (N.B. we retry because this could be racy...)
#                 retries -= 1
#
#             await asyncio.sleep(2.0)
#
#         return result  # noqa
#
#
#     async def cook(self, kitchen: Kitchen) -> None:
#         # apt-installs built up the args to represent what was needed, so this
#         # will work as-is
#         kitchen.get_dependency_tracker()
#
#         if self.packages:
#             update = await self._apt_command(kitchen, ["apt-get", "-yq", "update"])
#             if update.exit_code != 0:
#                 raise RuntimeError(
#                     f"apt update failed with err {update.exit_code}: {update.stderr!r}"
#                 )
#
#             install_args = ["apt-get", "-yq", "install"]
#             install_args += list(self.packages)
#             install = await self._apt_command(kitchen, install_args)
#
#             if install.exit_code != 0:
#                 raise RuntimeError(
#                     f"apt install failed with err {install.exit_code}:"
#                     f" {install.stderr!r}"
#                 )


class AptPackage(Recipe):
    _NAME = "apt-install"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)
        self.packages: List[str] = check_type(args["packages"], list)

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)

        for package in self.packages:
            preparation.provides("apt-package", package)

    async def _apt_command(
        self, kitchen: Kitchen, args: List[str]
    ) -> SimpleExec.Result:
        retries = 3

        while retries > 0:
            result = await kitchen.ut1areq(SimpleExec(args, "/"), SimpleExec.Result)

            if result.exit_code == 0 or b"/lock" not in result.stderr:
                return result

            logger.warning(
                "Failed apt command due to suspected locking issue. Will retry…"
            )

            retries -= 1

            # /lock seen in stderr, probably a locking issue...
            lock_check = await kitchen.ut1areq(
                SimpleExec(
                    ["fuser", "/var/lib/dpkg/lock", "/var/lib/apt/lists/lock"], "/"
                ),
                SimpleExec.Result,
            )

            if lock_check.exit_code != 0:
                # non-zero code means the file is not being accessed;
                # use up a retry (N.B. we retry because this could be racy...)
                logger.warning(
                    "Suspected locking issue is either racy or a red herring."
                )
                retries -= 1

            await asyncio.sleep(2.0)

        return result  # noqa

    async def cook(self, kitchen: Kitchen) -> None:
        # this is a one-off task assuming everything works
        kitchen.get_dependency_tracker()

        if self.packages:
            update = await self._apt_command(kitchen, ["apt-get", "-yq", "update"])
            if update.exit_code != 0:
                raise RuntimeError(
                    f"apt update failed with err {update.exit_code}: {update.stderr!r}"
                )

            install_args = ["apt-get", "-yq", "install"]
            install_args += list(self.packages)
            install = await self._apt_command(kitchen, install_args)

            if install.exit_code != 0:
                raise RuntimeError(
                    f"apt install failed with err {install.exit_code}:"
                    f" {install.stderr!r}"
                )
