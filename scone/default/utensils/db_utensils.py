import logging

import attr

try:
    import asyncpg
except ImportError:
    asyncpg = None

from scone.common.chanpro import Channel
from scone.sous import Utensil
from scone.sous.utensils import Worktop

logger = logging.getLogger(__name__)

if not asyncpg:
    logger.info("asyncpg not found, install if you need Postgres support")


@attr.s(auto_attribs=True)
class PostgresTransaction(Utensil):
    database: str

    # statements like CREATE DATABASE are not permitted in transactions.
    use_transaction_block: bool = True

    async def execute(self, channel: Channel, worktop: Worktop) -> None:
        if not asyncpg:
            raise RuntimeError("asyncpg is not installed.")

        async def queryloop():
            while True:
                next_input = await channel.recv()
                if next_input is None:
                    return
                query, *args = next_input
                if query is None:
                    break
                try:
                    results = [
                        dict(record) for record in await conn.fetch(query, *args)
                    ]
                except asyncpg.PostgresError:
                    logger.error(
                        "Failed query %s with args %r", query, args, exc_info=True
                    )
                    await channel.close("Query error")
                    raise

                await channel.send(results)

        conn = await asyncpg.connect(database=self.database)
        try:
            if self.use_transaction_block:
                async with conn.transaction():
                    await queryloop()
            else:
                await queryloop()
        finally:
            await conn.close()
