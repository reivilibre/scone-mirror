from scone.default.utensils.docker_utensils import (
    DockerContainerRun,
    DockerVolumeCreate,
)
from scone.head.kitchen import Kitchen
from scone.head.recipe import Recipe, RecipeContext
from scone.head.utils import check_type


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


class DockerVolume(Recipe):
    _NAME = "docker-volume"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.name = check_type(args.get("name"), str)

    async def cook(self, kitchen: Kitchen) -> None:
        kitchen.get_dependency_tracker()
        await kitchen.ut1areq(DockerVolumeCreate(self.name), DockerVolumeCreate.Result)
