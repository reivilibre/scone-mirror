import asyncio
import sys
from argparse import ArgumentParser


def cli() -> None:
    code = asyncio.get_event_loop().run_until_complete(cli_async())
    sys.exit(code)


async def cli_async() -> int:
    args = sys.argv[1:]

    parser = ArgumentParser(description="Compose a menu!")
    subs = parser.add_subparsers()
    supermarket = subs.add_parser("supermarket", help="generate a [[supermarket]] dish")
    supermarket.add_argument("url", help="HTTPS URL to download")
    supermarket.add_argument("-a", "--as", help="Alternative filename")
    supermarket.set_defaults(func=supermarket_cli)

    argp = parser.parse_args(args)
    return await argp.func(argp)


async def supermarket_cli(argp) -> int:
    return 0  # TODO
