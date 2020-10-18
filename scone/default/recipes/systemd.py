from scone.default.steps.systemd_steps import (
    cook_systemd_daemon_reload,
    cook_systemd_enable,
    cook_systemd_start,
)
from scone.head.head import Head
from scone.head.kitchen import Kitchen, Preparation
from scone.head.recipe import Recipe, RecipeContext
from scone.head.utils import check_type, check_type_opt


class SystemdUnit(Recipe):
    """
    System unit.
    TODO(performance): make it collapsible in a way so that it can daemon-reload
        only once in most situations.
    """

    _NAME = "systemd"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        unit = check_type(args.get("unit"), str)
        self.unit_name = unit if "." in unit else unit + ".service"
        self.at = check_type(args.get("at"), str)
        self.enabled = check_type_opt(args.get("enabled"), bool)
        self.restart_on = check_type_opt(args.get("restart_on"), list)
        self.started = check_type_opt(args.get("started"), bool)

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)
        # TODO(potential future): preparation.provides("systemd-unit", self.unit_name)
        preparation.needs("file", self.at)

    async def cook(self, kitchen: Kitchen) -> None:
        if self.enabled is not None or self.started is not None:
            await cook_systemd_daemon_reload(kitchen)

        if self.enabled is not None:
            await cook_systemd_enable(kitchen, self.enabled, self.unit_name)

        if self.started is not None:
            if self.started:
                await cook_systemd_start(kitchen, self.unit_name)
