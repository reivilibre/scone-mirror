import attr
import docker.errors

from scone.common.chanpro import Channel
from scone.sous import Utensil
from scone.sous.utensils import Worktop

_docker_client_instance = None


def _docker_client():
    global _docker_client_instance
    if not _docker_client_instance:
        _docker_client_instance = docker.from_env()
    return _docker_client_instance


@attr.s(auto_attribs=True)
class DockerContainerRun(Utensil):
    image: str
    command: str

    @attr.s(auto_attribs=True)
    class Result:
        name: str

    async def execute(self, channel: Channel, worktop: Worktop):
        try:
            container = _docker_client().containers.run(
                self.image, self.command, detach=True
            )

        except docker.errors.ImageNotFound:
            # specified image does not exist (or requires login)
            await channel.send(None)
            return
        except docker.errors.APIError:
            # the docker server returned an error
            await channel.send(None)
            return

        await channel.send(DockerContainerRun.Result(name=container.name))
