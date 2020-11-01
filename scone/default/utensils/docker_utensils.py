from typing import Optional

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


@attr.s(auto_attribs=True)
class DockerVolumeCreate(Utensil):
    name: str
      
    @attr.s(auto_attribs=True)
    class Result:
        name: str

    async def execute(self, channel: Channel, worktop: Worktop):
        try:
            volume = _docker_client().volume.create(self.name)
        except docker.errors.APIError:
            # the docker server returned an error
            await channel.send(None)
            return
          
        await channel.send(DockerVolumeCreate.Result(name=volume.name))


@attr.s(auto_attribs=True)
class DockerNetworkCreate(Utensil):
    name: str
    check_duplicate: Optional[bool]
    internal: Optional[bool]
    enable_ipv6: Optional[bool]
    attachable: Optional[bool]
    ingress: Optional[bool]
      
    @attr.s(auto_attribs=True)
    class Result:
        name: str

    async def execute(self, channel: Channel, worktop: Worktop):
        try:
            network = _docker_client().networks.create(
                self.name,
                check_duplicate=self.check_duplicate,
                internal=self.internal,
                enable_ipv6=self.enable_ipv6,
                attachable=self.attachable,
                ingress=self.ingress,
            )
        except docker.errors.APIError:
            # the docker server returned an error
            await channel.send(None)
            return
          
        await channel.send(DockerContainerRun.Result(name=network.name))
