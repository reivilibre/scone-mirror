from pathlib import Path
from typing import Tuple, List

from scone.default.recipes.apt import AptPackage
from scone.default.steps.basic_steps import exec_no_fails
from scone.default.steps.filesystem_steps import depend_remote_file
from scone.head import Head, Recipe
from scone.head.kitchen import Kitchen
from scone.head.recipe import Preparation
from scone.head.utils import check_type


class PythonVenv(Recipe):
    """
    Creates a Python virtualenv with a specified set of requirements.

    Note: using a directory as a dependency can be inefficient as dir SHA256
    will be performed to check it has not changed.
    """

    _NAME = "python-venv"

    def __init__(self, host: str, slug: str, args: dict, head: Head):
        super().__init__(host, slug, args, head)

        self.dir = check_type(args.get("dir"), str)
        self.interpreter = check_type(args.get("interpreter"), str)
        # list of flags. Current supported:
        # git (local git repo — track hash by git commit hash), dir, -r
        self.install: List[Tuple[str, List[str]]] = []
        install_plaintext = check_type(args.get("install"), list)
        for install_line in install_plaintext:
            parts = install_line.split(" ")
            self.install.append((parts[-1], parts[0:-1]))

        self.no_apt_install = check_type(args.get("_no_apt_install", False), bool)

        # TODO(sdists)

    def prepare(self, preparation: Preparation, head: Head):
        super().prepare(preparation, head)
        preparation.needs("dir", str(Path(self.dir).parent))

        for name, flags in self.install:
            if "-r" in flags:
                preparation.needs("file", name)
            elif "git" in flags or "dir" in flags:
                preparation.needs("dir", name)

        final_script = str(Path(self.dir, "bin/python"))
        preparation.provides("file", str(final_script))

        if not self.no_apt_install:
            preparation.subrecipe(
                AptPackage(
                    self.get_host(), "@venv-apt", {"packages": ["python3-venv"]}, head
                )
            )
            preparation.needs("apt-stage", "packages-installed")

    async def cook(self, kitchen: Kitchen):
        dt = kitchen.get_dependency_tracker()

        await exec_no_fails(
            kitchen, [self.interpreter, "-m", "venv", self.dir], "/"
        )

        install_args = []
        for name, flags in self.install:
            if "-r" in flags:
                install_args.append("-r")
                await depend_remote_file(name, kitchen)
            elif "dir" in flags or "git" in flags:
                # TODO(perf, dedup): custom dynamic dependency types; git
                #   dependencies and sha256_dir dependencies.
                dt.ignore()

            install_args.append(name)

        await exec_no_fails(
            kitchen, [self.dir + "/bin/pip", "install"] + install_args, "/"
        )
