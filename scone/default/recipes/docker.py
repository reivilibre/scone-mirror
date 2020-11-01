from scone.default.utensils.docker_utensils import (
    DockerContainerRun,
    DockerNetworkCreate,
)
from scone.head.kitchen import Kitchen
from scone.head.recipe import Recipe, RecipeContext
from scone.head.utils import check_type, check_type_opt


class DockerContainer(Recipe):
    _NAME = "docker-container"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.image = check_type(args.get("image"), str)
        self.command = check_type(args.get("command"), str)

    async def cook(self, kitchen: Kitchen) -> None:
        kitchen.get_dependency_tracker()
        await kitchen.ut1areq(
            DockerContainerRun(self.image, self.command), DockerContainerRun.Result
        )


class DockerNetwork(Recipe):
    _NAME = "docker-network"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.name = check_type(args.get("name"), str)
        self.driver = check_type_opt(args.get("driver"), str)
        self.check_duplicate = check_type_opt(args.get("check_duplicate"), bool)
        self.internal = check_type_opt(args.get("internal"), bool)
        self.enable_ipv6 = check_type_opt(args.get("enable_ipv6"), bool)
        self.attachable = check_type_opt(args.get("attachable"), bool)
        self.scope = check_type_opt(args.get("scope"), str)
        self.ingress = check_type_opt(args.get("ingress"), bool)

    async def cook(self, kitchen: Kitchen) -> None:
        kitchen.get_dependency_tracker()
        await kitchen.ut1areq(
            DockerNetworkCreate(
                self.name,
                self.check_duplicate,
                self.internal,
                self.enable_ipv6,
                self.attachable,
                self.ingress,
            ),
            DockerNetworkCreate.Result,
        )
