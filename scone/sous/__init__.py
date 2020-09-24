from os import path

import toml

from scone.common.loader import ClassLoader
from scone.sous.utensils import Utensil, utensil_namer


class Sous:
    def __init__(self, ut_loader: ClassLoader[Utensil]):
        self.utensil_loader = ut_loader

    @staticmethod
    def open(directory: str):
        with open(path.join(directory, "scone.sous.toml")) as sous_toml:
            sous_data = toml.load(sous_toml)

        utensil_module_roots = sous_data.get(
            "utensil_roots", ["scone.default.utensils"]
        )

        # load available recipes
        loader: ClassLoader[Utensil] = ClassLoader(Utensil, utensil_namer)
        for package_root in utensil_module_roots:
            loader.add_package_root(package_root)

        return Sous(loader)
