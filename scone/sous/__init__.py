#  Copyright 2020, Olivier 'reivilibre'.
#
#  This file is part of Scone.
#
#  Scone is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Scone is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Scone.  If not, see <https://www.gnu.org/licenses/>.

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
