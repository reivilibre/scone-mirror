from scone.default.utensils.basic_utensils import HashFile
from scone.head.kitchen import Kitchen


async def depend_remote_file(path: str, kitchen: Kitchen) -> None:
    sha256 = await kitchen.ut1(HashFile(path))
    kitchen.get_dependency_tracker().register_remote_file(path, sha256)
