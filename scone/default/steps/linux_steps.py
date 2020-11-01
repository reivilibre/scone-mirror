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

from typing import Optional

from scone.default.utensils.basic_utensils import SimpleExec
from scone.head.kitchen import Kitchen


async def create_linux_user(
    kitchen: Kitchen,
    name: str,
    password_hash: Optional[str],
    create_home: bool = True,
    create_group: bool = True,
    home: Optional[str] = None,
) -> None:
    args = ["useradd"]

    if password_hash:
        # N.B. if you don't use a password hash, the account will be locked
        # but passwordless SSH still works
        args += ["-p", password_hash]

    if create_home:
        args.append("-m")

    if create_group:
        args.append("-U")
    else:
        args.append("-N")

    if home:
        args += ["-d", home]

    # finally, append the user name
    args.append(name)

    result = await kitchen.ut1areq(SimpleExec(args, "/"), SimpleExec.Result)

    if result.exit_code != 0:
        raise RuntimeError(
            "Failed to create user. Error was: " + result.stderr.strip().decode()
        )
