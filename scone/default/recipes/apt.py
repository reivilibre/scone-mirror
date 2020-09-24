from typing import Dict, List, Set, Tuple

from scone.default.utensils.basic_utensils import SimpleExec
from scone.head import Head, Recipe
from scone.head.kitchen import Kitchen
from scone.head.recipe import Preparation
from scone.head.utils import check_type


class AptInstallInternal(Recipe):
    """
    Actually installs the packages; does it in a single batch for efficiency!
    """

    _NAME = "apt-install.internal"

    # TODO(extension, low): expand this into apt-install-now if we need
    #     the flexibility

    def __init__(self, host: str, slug: str, args: dict, head: "Head"):
        super().__init__(host, slug, args, head)

        self.packages: Set[str] = set()

        args["packages"] = self.packages
        args[".source"] = ("@virtual", "apt-install-internal", "the one true AII")

    def get_user(self, head: "Head") -> str:
        return "root"

    def prepare(self, preparation: Preparation, head: "Head") -> None:
        super().prepare(preparation, head)
        preparation.needs("apt-stage", "internal-install-packages")
        preparation.needs("apt-stage", "repositories-declared")
        preparation.provides("apt-stage", "packages-installed")

    async def cook(self, kitchen: Kitchen) -> None:
        # apt-installs built up the args to represent what was needed, so this
        # will work as-is
        kitchen.get_dependency_tracker()

        if self.packages:
            update = await kitchen.ut1areq(
                SimpleExec(["apt-get", "-yq", "update"], "/"), SimpleExec.Result
            )
            if update.exit_code != 0:
                raise RuntimeError(
                    f"apt update failed with err {update.exit_code}: {update.stderr!r}"
                )

            install_args = ["apt-get", "-yq", "install"]
            install_args += list(self.packages)
            install = await kitchen.ut1areq(
                SimpleExec(install_args, "/"), SimpleExec.Result
            )

            if install.exit_code != 0:
                raise RuntimeError(
                    f"apt install failed with err {install.exit_code}:"
                    f" {install.stderr!r}"
                )


class AptPackage(Recipe):
    _NAME = "apt-install"

    internal_installers: Dict[Tuple[Head, str], AptInstallInternal] = {}

    def __init__(self, host: str, slug: str, args: dict, head: Head):
        super().__init__(host, slug, args, head)
        self.packages: List[str] = check_type(args["packages"], list)

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)
        pair = (head, self.get_host())
        if pair not in AptPackage.internal_installers:
            install_internal = AptInstallInternal(self.get_host(), "internal", {}, head)
            AptPackage.internal_installers[pair] = install_internal
            preparation.subrecipe(install_internal)
        preparation.provides("apt-stage", "internal-install-packages")

        internal_installer = AptPackage.internal_installers.get(pair)
        assert internal_installer is not None
        internal_installer.packages.update(self.packages)

    async def cook(self, kitchen: Kitchen) -> None:
        # can't be tracked
        kitchen.get_dependency_tracker().ignore()
