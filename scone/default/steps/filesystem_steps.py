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

from scone.default.utensils.basic_utensils import HashFile
from scone.head.kitchen import Kitchen


async def depend_remote_file(path: str, kitchen: Kitchen) -> None:
    sha256 = await kitchen.ut1(HashFile(path))
    kitchen.get_dependency_tracker().register_remote_file(path, sha256)
