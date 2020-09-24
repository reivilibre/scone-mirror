import crypt
import logging
from typing import Optional

from scone.default.steps import linux_steps
from scone.default.utensils.linux_utensils import GetPasswdEntry
from scone.head import Head, Recipe
from scone.head.kitchen import Kitchen
from scone.head.recipe import Preparation
from scone.head.utils import check_type, check_type_opt

logger = logging.getLogger(__name__)


class LinuxUser(Recipe):
    _NAME = "os-user"

    def __init__(self, host: str, slug: str, args: dict, head: Head):
        super().__init__(host, slug, args, head)
        if slug[0] == "@":
            raise ValueError("os-user should be used like [os-user.username].")

        self.user_name = slug
        self.make_group = check_type(args.get("make_group", True), bool)
        self.make_home = check_type(args.get("make_home", True), bool)
        self.home: Optional[str] = check_type_opt(args.get("home"), str)
        self.password: Optional[str] = check_type_opt(args.get("password"), str)

    def prepare(self, preparation: Preparation, head: "Head") -> None:
        super().prepare(preparation, head)
        preparation.provides("os-user", self.user_name)
        if self.make_group:
            preparation.provides("os-group", self.user_name)

    async def cook(self, kitchen: Kitchen) -> None:
        # TODO(documentation): note this does not update users
        # acknowledge tracking
        kitchen.get_dependency_tracker()
        if self.password:
            password_hash: Optional[str] = crypt.crypt(self.password)
        else:
            password_hash = None

        pwd_entry = await kitchen.ut1a(
            GetPasswdEntry(self.user_name), GetPasswdEntry.Result
        )

        if pwd_entry:
            logger.warning(
                "Not updating existing os-user '%s' as it exists already and "
                "modifications could be dangerous in any case. Modification "
                "support may be implemented in the future.",
                self.user_name,
            )
        else:
            # create the user fresh
            await linux_steps.create_linux_user(
                kitchen,
                self.user_name,
                password_hash,
                self.make_home,
                self.make_group,
                self.home,
            )
