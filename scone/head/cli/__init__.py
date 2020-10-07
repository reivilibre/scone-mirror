import asyncio
import logging
import os
import sys
import time
from argparse import ArgumentParser
from pathlib import Path

from scone.common.misc import eprint
from scone.common.pools import Pools
from scone.head import dot_emitter
from scone.head.dependency_tracking import DependencyCache
from scone.head.head import Head
from scone.head.kitchen import Kitchen, Preparation


def cli() -> None:
    logging.basicConfig()
    logging.getLogger("scone").setLevel(logging.DEBUG)
    code = asyncio.get_event_loop().run_until_complete(cli_async())
    sys.exit(code)


async def cli_async() -> int:
    dep_cache = None
    try:
        args = sys.argv[1:]

        parser = ArgumentParser(description="Cook!")
        parser.add_argument("hostspec", type=str, help="Sous or group name")
        parser.add_argument(
            "--yes",
            "-y",
            action="store_true",
            default=False,
            help="Don't prompt for confirmation",
        )
        argp = parser.parse_args(args)

        eprint("Loading head…")

        cdir = Path(os.getcwd())

        while not Path(cdir, "scone.head.toml").exists():
            cdir = cdir.parent
            if len(cdir.parts) <= 1:
                eprint("Don't appear to be in a head. STOP.")
                return 1

        head = Head.open(str(cdir))

        eprint(head.debug_info())

        hosts = set()

        if argp.hostspec in head.souss:
            hosts.add(argp.hostspec)
        elif argp.hostspec in head.groups:
            for sous in head.groups[argp.hostspec]:
                hosts.add(sous)
        else:
            eprint(f"Unrecognised sous or group: '{argp.hostspec}'")
            sys.exit(1)

        eprint(f"Selected the following souss: {', '.join(hosts)}")

        eprint("Preparing recipes…")
        prepare = Preparation(head)

        start_ts = time.monotonic()
        prepare.prepare_all()
        del prepare
        end_ts = time.monotonic()
        eprint(f"Preparation completed in {end_ts - start_ts:.3f} s.")
        # eprint(f"{len(order)} courses planned.")

        dot_emitter.emit_dot(head.dag, Path(cdir, "dag.0.dot"))

        dep_cache = await DependencyCache.open(
            os.path.join(head.directory, "depcache.sqlite3")
        )

        # eprint("Checking dependency cache…")
        # start_ts = time.monotonic()
        # depchecks = await run_dep_checks(head, dep_cache, order)
        # end_ts = time.monotonic()
        # eprint(f"Checking finished in {end_ts - start_ts:.3f} s.")  # TODO show counts
        #
        # for epoch, items in enumerate(order):
        #     print(f"----- Course {epoch} -----")
        #
        #     for item in items:
        #         if isinstance(item, Recipe):
        #             state = depchecks[item].label.name
        #             print(f" > recipe ({state}) {item}")
        #         elif isinstance(item, tuple):
        #             kind, ident, extra = item
        #             print(f" - we now have {kind} {ident} {dict(extra)}")

        eprint("Ready to cook? [y/N]: ", end="")
        if argp.yes:
            eprint("y (due to --yes)")
        else:
            if not input().lower().startswith("y"):
                eprint("Stopping.")
                return 101

        kitchen = Kitchen(head, dep_cache)

        # for epoch, epoch_items in enumerate(order):
        #     print(f"Cooking Course {epoch} of {len(order)}")
        #     await kitchen.run_epoch(
        #         epoch_items, depchecks, concurrency_limit_per_host=8
        #     )
        #
        # for sous in hosts: TODO this is not definitely safe
        #     await dep_cache.sweep_old(sous)

        try:
            await kitchen.cook_all()
        finally:
            dot_emitter.emit_dot(head.dag, Path(cdir, "dag.9.dot"))

        return 0
    finally:
        Pools.get().shutdown()
        if dep_cache:
            await dep_cache.db.close()


if __name__ == "__main__":
    cli()
