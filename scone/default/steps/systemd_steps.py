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
