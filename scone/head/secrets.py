from typing import Optional

import nacl
import nacl.utils
import secretstorage
from nacl.encoding import RawEncoder, URLSafeBase64Encoder
from nacl.secret import SecretBox

from scone.common.misc import eprint


class SecretAccess:
    def __init__(self, restaurant_identifier: Optional[str]):
        self.restaurant_identifier = restaurant_identifier
        self.key: Optional[bytes] = None

    def encrypt_bytes(self, data: bytes, encoder=RawEncoder) -> bytes:
        box = SecretBox(self.key)
        return box.encrypt(data, None, encoder)

    def decrypt_bytes(self, data: bytes, encoder=RawEncoder) -> bytes:
        box = SecretBox(self.key)
        return box.decrypt(data, None, encoder)

    def generate_new(self):
        eprint("Generating a new freezer key...")
        self.key = nacl.utils.random(SecretBox.KEY_SIZE)
        key_b64 = URLSafeBase64Encoder.encode(self.key)
        eprint("Your new key is: " + key_b64.decode())
        eprint("Pretty please store it in a safe place!")

        if not self.restaurant_identifier:
            eprint("No RI; not saving to SS")
            return

        eprint("Attempting to save it to the secret service...")
        eprint("(save it yourself anyway!)")

        with secretstorage.dbus_init() as connection:
            collection = secretstorage.get_default_collection(connection)
            attributes = {
                "application": "Scone",
                "restaurant": self.restaurant_identifier,
            }
            items = list(collection.search_items(attributes))
            if items:
                eprint(
                    "Found secret sauce for this Restaurant already!"
                    " Will not overwrite."
                )
            else:
                eprint("Storing secret sauce for this Restaurant...")
                collection.create_item(
                    f"scone({self.restaurant_identifier}): secret sauce",
                    attributes,
                    key_b64,
                )
                eprint("OK!")

    def get_existing(self):
        if self.restaurant_identifier is not None:
            self.key = self._try_dbus_auth(self.restaurant_identifier)
        else:
            self.key = self._try_manual_entry()

    def _try_dbus_auth(self, restaurant_identifier: str) -> Optional[bytes]:
        eprint("Trying D-Bus Secret Service")
        try:
            with secretstorage.dbus_init() as connection:
                collection = secretstorage.get_default_collection(connection)
                attributes = {
                    "application": "Scone",
                    "restaurant": restaurant_identifier,
                }
                items = list(collection.search_items(attributes))
                if items:
                    eprint("Found secret sauce for this Restaurant, unlocking…")
                    items[0].unlock()
                    return URLSafeBase64Encoder.decode(items[0].get_secret())
                else:
                    eprint("Did not find secret sauce for this Restaurant.")
                    eprint("Enter it and I will try and store it...")
                    secret = self._try_manual_entry()
                    if secret is not None:
                        collection.create_item(
                            f"scone({restaurant_identifier}): secret sauce",
                            attributes,
                            URLSafeBase64Encoder.encode(secret),
                        )
                        return secret
                    return None
        except EOFError:  # XXX what happens with no D-Bus
            return None

    def _try_manual_entry(self) -> Optional[bytes]:
        eprint("Manual entry required. Enter password for this restaurant: ", end="")
        key = URLSafeBase64Encoder.decode(input().encode())
        if len(key) != SecretBox.KEY_SIZE:
            eprint("Wrong size!")
            return None
        else:
            return key
