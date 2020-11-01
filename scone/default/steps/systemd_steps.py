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

from scone.default.utensils.basic_utensils import SimpleExec
from scone.head.kitchen import Kitchen


async def cook_systemd_enable(kitchen: Kitchen, enabled: bool, unit_name: str):
    # thoughts: we could find the unit path with:
    #   systemctl show -p FragmentPath apache2.service

    result = await kitchen.ut1areq(
        SimpleExec(["systemctl", "enable" if enabled else "disable", unit_name], "/",),
        SimpleExec.Result,
    )

    if result.exit_code != 0:
        raise RuntimeError(
            f"Failed to en/disable {unit_name}: {result.stderr.decode()}"
        )


async def cook_systemd_daemon_reload(kitchen):
    result = await kitchen.ut1areq(
        SimpleExec(["systemctl", "daemon-reload"], "/",), SimpleExec.Result,
    )

    if result.exit_code != 0:
        raise RuntimeError(f"Failed to reload systemd: {result.stderr.decode()}")


async def cook_systemd_start(kitchen: Kitchen, unit_name: str):
    result = await kitchen.ut1areq(
        SimpleExec(["systemctl", "start", unit_name], "/",), SimpleExec.Result,
    )

    if result.exit_code != 0:
        raise RuntimeError(f"Failed to start {unit_name}: {result.stderr.decode()}")
