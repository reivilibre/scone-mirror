#  Copyright 2020, Olivier 'reivilibre'.
#
#  This file is part of Scone.
#
#  Scone is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Scone is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Scone.  If not, see <https://www.gnu.org/licenses/>.

from pathlib import Path
from typing import List

from scone.common.modeutils import DEFAULT_MODE_DIR, parse_mode
from scone.default.steps.basic_steps import exec_no_fails
from scone.default.steps.filesystem_steps import depend_remote_file
from scone.default.utensils.basic_utensils import (
    Chmod,
    Chown,
    MakeDirectory,
    SimpleExec,
    Stat,
)
from scone.default.utensils.dynamic_dependencies import HasChangedInSousStore
from scone.head.head import Head
from scone.head.kitchen import Kitchen, Preparation
from scone.head.recipe import Recipe, RecipeContext
from scone.head.utils import check_type, check_type_opt


class DeclareFile(Recipe):
    """
    Declares that a file already exists on the sous.

    Maybe we will assert it in the future?
    """

    _NAME = "declare-file"

    def prepare(self, preparation: Preparation, head: Head):
        preparation.provides("file", self.arguments["path"])

    async def cook(self, kitchen: Kitchen):
        # mark as tracked.
        kitchen.get_dependency_tracker()


class DeclareDirectory(Recipe):
    """
    Declares that a directory already exists on the sous.

    Maybe we will assert it in the future?
    """

    _NAME = "declare-dir"

    def prepare(self, preparation: Preparation, head: Head):
        preparation.provides("directory", self.arguments["path"])

    async def cook(self, kitchen: Kitchen):
        # mark as tracked.
        kitchen.get_dependency_tracker()


class EnsureDirectory(Recipe):
    """
    Makes a directory tree.
    """

    _NAME = "directory"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)
        parents = args.get("parents", 0)
        assert isinstance(parents, int)

        path = args.get("path")
        assert isinstance(path, str)

        mode = args.get("mode", DEFAULT_MODE_DIR)
        assert isinstance(mode, str) or isinstance(mode, int)

        self.path = path
        self.parents = parents
        self.mode = parse_mode(mode, directory=True)
        self._make: List[str] = []
        self.targ_user = args.get("owner", recipe_context.user)
        self.targ_group = args.get("group", self.targ_user)

    def prepare(self, preparation: Preparation, head: "Head"):
        super().prepare(preparation, head)
        preparation.needs("os-user", self.targ_user)
        preparation.needs("os-group", self.targ_group)
        preparation.provides("directory", self.path)
        self._make.append(self.path)
        parent = Path(self.path).parent
        for _ in range(self.parents):
            self._make.append(str(parent))
            preparation.provides("directory", str(parent))
            parent = parent.parent
        preparation.needs("directory", str(parent))
        self._make.reverse()

    async def cook(self, k: Kitchen):
        for directory in self._make:
            stat = await k.ut1a(Stat(directory), Stat.Result)
            if stat is None:
                # doesn't exist, make it
                await k.ut0(MakeDirectory(directory, self.mode))

            stat = await k.ut1a(Stat(directory), Stat.Result)
            if stat is None:
                raise RuntimeError("Directory vanished after creation!")

            if stat.dir:
                if (stat.user, stat.group) != (self.targ_user, self.targ_group):
                    # need to chown
                    await k.ut0(Chown(directory, self.targ_user, self.targ_group))

                if stat.mode != self.mode:
                    await k.ut0(Chmod(directory, self.mode))
            else:
                raise RuntimeError("Already exists but not a dir: " + directory)

        # mark as tracked.
        k.get_dependency_tracker()


class ExtractTar(Recipe):
    """
    Extracts a tar archive, expecting to get at least some files.
    """

    _NAME = "tar-extract"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.tar = check_type(args.get("tar"), str)
        self.dir = check_type(args.get("dir"), str)
        self.expect_files = check_type(args.get("expects_files"), List[str])

    def prepare(self, preparation: Preparation, head: "Head"):
        super().prepare(preparation, head)
        preparation.needs("file", self.tar)
        preparation.needs("directory", self.dir)
        for file in self.expect_files:
            assert isinstance(file, str)
            final = str(Path(self.dir, file))
            preparation.provides("file", final)

    async def cook(self, k: "Kitchen"):
        res = await k.ut1areq(
            SimpleExec(["tar", "xf", self.tar], self.dir), SimpleExec.Result
        )
        if res.exit_code != 0:
            raise RuntimeError(
                f"tar failed with ec {res.exit_code}; stderr = <<<"
                f"\n{res.stderr.decode()}\n>>>"
            )

        for expect_relative in self.expect_files:
            expect = str(Path(self.dir, expect_relative))
            stat = await k.ut1a(Stat(expect), Stat.Result)
            if stat is None:
                raise RuntimeError(
                    f"tar succeeded but expectation failed; {expect!r} not found."
                )


class RunScript(Recipe):
    """
    Runs a script (such as an installation script).
    """

    _NAME = "script-run"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.working_dir = check_type(args.get("working_dir"), str)

        # relative to working dir
        self.script = check_type(args.get("script"), str)

        # todo other remote dependencies
        # todo provided files as a result of the script exec

    def prepare(self, preparation: Preparation, head: "Head"):
        super().prepare(preparation, head)
        final_script = str(Path(self.working_dir, self.script))
        preparation.needs("file", final_script)

        # TODO more needs
        # TODO preparation.provides()

    async def cook(self, kitchen: "Kitchen"):
        final_script = str(Path(self.working_dir, self.script))
        await depend_remote_file(final_script, kitchen)


class CommandOnChange(Recipe):
    """
    Runs a command when at least one file listed has changed on the remote.
    """

    _NAME = "command-on-change"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.purpose = check_type(args.get("purpose"), str)
        self.command = check_type(args.get("command"), list)
        self.watching = check_type(args.get("files"), list)
        self.working_dir = check_type(args.get("working_dir", "/"), str)

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)
        for file in self.watching:
            preparation.needs("file", file)

    async def cook(self, kitchen: Kitchen) -> None:
        kitchen.get_dependency_tracker().ignore()

        changed = await kitchen.ut1(HasChangedInSousStore(self.purpose, self.watching))

        if changed:
            result = await kitchen.ut1areq(
                SimpleExec(self.command, self.working_dir), SimpleExec.Result
            )

            if result.exit_code != 0:
                raise RuntimeError(
                    f"exit code not 0 ({result.exit_code}), {result.stderr!r}"
                )


class GitCheckout(Recipe):
    _NAME = "git"

    # TODO(correctness): branches can change (tags too), but this will still
    #     declare SAFE_TO_SKIP. Perhaps we want to stop that unless you opt out?
    #     But oh well for now.

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.repo_src = check_type(args.get("src"), str)
        self.dest_dir = check_type(args.get("dest"), str)
        self.ref = check_type_opt(args.get("ref"), str)
        self.branch = check_type_opt(args.get("branch"), str)

        if not (self.ref or self.branch):
            raise ValueError("Need to specify 'ref' or 'branch'")

        if self.ref and self.branch:
            raise ValueError("Can't specify both 'ref' and 'branch'.")

        # should end with / if it's a dir
        self.expect: List[str] = check_type(args.get("expect", []), list)
        self.submodules = check_type(args.get("submodules", False), bool)

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)
        parent = str(Path(self.dest_dir).parent)
        preparation.needs("directory", parent)
        preparation.provides("directory", self.dest_dir)

        for expected in self.expect:
            expected_path_str = str(Path(self.dest_dir, expected))
            if expected.endswith("/"):
                preparation.provides("directory", expected_path_str)
            else:
                preparation.provides("file", expected_path_str)

    async def cook(self, k: Kitchen) -> None:
        # no non-arg dependencies
        k.get_dependency_tracker()

        stat = await k.ut1a(Stat(self.dest_dir), Stat.Result)
        if stat is None:
            # doesn't exist; git init it
            await exec_no_fails(k, ["git", "init", self.dest_dir], "/")

        stat = await k.ut1a(Stat(self.dest_dir), Stat.Result)
        if stat is None:
            raise RuntimeError("Directory vanished after creation!")

        if not stat.dir:
            raise RuntimeError("Already exists but not a dir: " + self.dest_dir)

        # add the remote, removing it first to ensure it's what we want
        # don't care if removing fails
        await k.ut1areq(
            SimpleExec(["git", "remote", "remove", "scone"], self.dest_dir),
            SimpleExec.Result,
        )
        await exec_no_fails(
            k, ["git", "remote", "add", "scone", self.repo_src], self.dest_dir
        )

        # fetch the latest from the remote
        await exec_no_fails(k, ["git", "fetch", "scone"], self.dest_dir)

        # figure out what ref we want to use
        # TODO(performance): fetch only this ref?
        ref = self.ref or f"scone/{self.branch}"

        # switch to that ref
        await exec_no_fails(k, ["git", "switch", "--detach", ref], self.dest_dir)

        # if we use submodules
        if self.submodules:
            await exec_no_fails(
                k,
                ["git", "submodule", "update", "--init", "--recursive"],
                self.dest_dir,
            )

        for expected in self.expect:
            expected_path_str = str(Path(self.dest_dir, expected))
            # TODO(performance, low): parallelise these
            stat = await k.ut1a(Stat(expected_path_str), Stat.Result)
            if not stat:
                raise RuntimeError(
                    f"expected {expected_path_str} to exist but it did not"
                )

            if stat.dir and not expected.endswith("/"):
                raise RuntimeError(
                    f"expected {expected_path_str} to exist as a file but it is a dir"
                )

            if not stat.dir and expected.endswith("/"):
                raise RuntimeError(
                    f"expected {expected_path_str} to exist as a dir but it is a file"
                )
