from scone.default.utensils.db_utensils import PostgresTransaction
from scone.head.head import Head
from scone.head.kitchen import Kitchen, Preparation
from scone.head.recipe import Recipe, RecipeContext
from scone.head.utils import check_type


class PostgresDatabase(Recipe):
    _NAME = "pg-db"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.database_name = check_type(args.get("name"), str)
        self.owner = check_type(args.get("owner"), str)
        self.encoding = args.get("encoding", "utf8")
        self.collate = args.get("collate", "en_GB.utf8")
        self.ctype = args.get("ctype", "en_GB.utf8")
        self.template = args.get("template", "template0")

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)
        # todo

    async def cook(self, kitchen: Kitchen) -> None:
        ch = await kitchen.start(PostgresTransaction("postgres"))
        await ch.send(
            (
                "SELECT 1 AS count FROM pg_catalog.pg_database WHERE datname = ?;",
                self.database_name,
            )
        )
        dbs = await ch.recv()
        if len(dbs) > 0 and dbs[0]["count"] == 1:
            await ch.send(None)
            await ch.wait_close()
            return

        q = f"""
            CREATE DATABASE {self.database_name}
                WITH OWNER {self.owner}
                ENCODING {self.encoding}
                LC_COLLATE {self.collate}
                LC_CTYPE {self.ctype}
                TEMPLATE {self.template};
        """

        await ch.send((q,))
        res = await ch.recv()
        if len(res) != 0:
            raise RuntimeError("expected empty result set.")
        await ch.send(None)
        await ch.wait_close()


class PostgresUser(Recipe):
    _NAME = "pg-user"

    def __init__(self, recipe_context: RecipeContext, args: dict, head):
        super().__init__(recipe_context, args, head)

        self.user_name = check_type(args.get("name"), str)
        self.password = check_type(args.get("password"), str)

    def prepare(self, preparation: Preparation, head: Head) -> None:
        super().prepare(preparation, head)
        # todo

    async def cook(self, kitchen: Kitchen) -> None:
        ch = await kitchen.start(PostgresTransaction("postgres"))
        await ch.send(
            (
                "SELECT 1 AS count FROM pg_catalog.pg_user WHERE usename = ?;",
                self.user_name,
            )
        )
        dbs = await ch.recv()
        if len(dbs) > 0 and dbs[0]["count"] == 1:
            await ch.send(None)
            await ch.wait_close()
            return

        q = f"""
            CREATE ROLE {self.user_name}
                WITH PASSWORD ?
                LOGIN;
        """

        await ch.send((q, self.password))
        res = await ch.recv()
        if len(res) != 0:
            raise RuntimeError("expected empty result set.")
        await ch.send(None)
        await ch.wait_close()
