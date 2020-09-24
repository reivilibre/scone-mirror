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
