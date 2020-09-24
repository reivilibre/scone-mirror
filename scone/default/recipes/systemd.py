from typing import Dict

from scone.default.recipes.filesystem import CommandOnChange
from scone.default.utensils.basic_utensils import SimpleExec
from scone.head import Head, Recipe
from scone.head.kitchen import Kitchen
from scone.head.recipe import Preparation
from scone.head.utils import check_type, check_type_opt


class SystemdUnit(Recipe):
    """
    Shorthand for a system unit. Metarecipe.
    """

    _NAME = "systemd"

    daemon_reloaders: Dict[str, CommandOnChange] = {}

    def __init__(self, host: str, slug: str, args: dict, head: Head):
        super().__init__(host, slug, args, head)

        self.unit_name = slug if "." in slug else slug + ".service"
        self.at = check_type(args.get("at"), str)
        self.enabled = check_type_opt(args.get("enabled"), bool)
        self.restart_on = check_type_opt(args.get("restart_on"), list)

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)
        preparation.provides("systemd-unit", self.unit_name)
        preparation.needs("systemd-stage", "daemon-reloaded")

        if self.enabled is not None:
            enable_recipe = SystemdEnabled(
                self.get_host(),
                self.unit_name,
                {"enabled": self.enabled, "at": self.at, ".user": "root"},
                head,
            )
            preparation.subrecipe(enable_recipe)
            preparation.needs("systemd-stage", "enabled")

        daemon_reloader = SystemdUnit.daemon_reloaders.get(self.get_host(), None)
        if not daemon_reloader:
            # TODO this should be replaced with a dedicated command which provides
            #   those units.
            daemon_reloader = CommandOnChange(
                self.get_host(),
                "systemd-internal",
                {
                    "purpose": "systemd.daemon_reload",
                    "command": ["systemctl", "daemon-reload"],
                    "files": [],
                    ".user": "root",
                },
                head,
            )
            preparation.subrecipe(daemon_reloader)
        file_list = getattr(daemon_reloader, "_args")["files"]
        file_list.append(self.at)

        if self.restart_on:
            service_reloader = CommandOnChange(
                self.get_host(),
                "systemd-internal",
                {
                    "purpose": "systemd.unit_reload",
                    "command": ["systemctl", "reload", self.unit_name],
                    "files": self.restart_on + [self.at],
                    ".user": "root",
                },
                head,
            )
            preparation.subrecipe(service_reloader)

    async def cook(self, kitchen: Kitchen) -> None:
        # metarecipes don't do anything.
        kitchen.get_dependency_tracker().ignore()


class SystemdEnabled(Recipe):
    """
    Sets the enabled state of the systemd unit.
    """

    _NAME = "systemd-enabled"

    def __init__(self, host: str, slug: str, args: dict, head: Head):
        super().__init__(host, slug, args, head)

        self.unit_name = slug if "." in slug else slug + ".service"
        self.at = check_type(args.get("at"), str)
        self.enabled = check_type_opt(args.get("enabled"), bool)

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)
        preparation.needs("file", self.at)
        preparation.needs("systemd-stage", "daemon-reloaded")

    async def cook(self, kitchen: Kitchen) -> None:
        kitchen.get_dependency_tracker()

        result = await kitchen.ut1areq(
            SimpleExec(
                ["systemctl", "enable" if self.enabled else "disable", self.unit_name],
                "/",
            ),
            SimpleExec.Result,
        )

        if result.exit_code != 0:
            raise RuntimeError(
                f"Failed to en/disable {self.unit_name}: {result.stderr.decode()}"
            )
