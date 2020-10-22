import attr

from scone.common.chanpro import Channel
from scone.sous import Utensil
from scone.sous.utensils import Worktop


@attr.s(auto_attribs=True)
class PostgresTransaction(Utensil):
    database: str

    async def execute(self, channel: Channel, worktop: Worktop) -> None:
        import asyncpg

        conn = await asyncpg.connect(database=self.database)
        try:
            async with conn.transaction():
                while True:
                    query, *args = await channel.recv()
                    if query is None:
                        break
                    results = [
                        dict(record) for record in await conn.fetch(query, *args)
                    ]

                    await channel.send(results)
        finally:
            await conn.close()
