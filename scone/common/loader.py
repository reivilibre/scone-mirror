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

import importlib
import pkgutil
from inspect import isclass
from typing import Any, Callable, Dict, Generic, Optional, TypeVar

T = TypeVar("T")


class ClassLoader(Generic[T]):
    def __init__(self, clarse: Any, name_getter: Callable[[Any], Optional[str]]):
        self._class = clarse
        self._classes: Dict[str, Callable[[str, str, dict], T]] = dict()
        self._name_getter = name_getter

    def add_package_root(self, module_root: str):
        module = importlib.import_module(module_root)
        self._add_module(module)

        # find subpackages
        for mod in pkgutil.iter_modules(module.__path__):  # type: ignore
            if mod.ispkg:
                self.add_package_root(module_root + "." + mod.name)
            else:
                submodule = importlib.import_module(module_root + "." + mod.name)
                self._add_module(submodule)

    def _add_module(self, module):
        # find recipes
        for name in dir(module):
            item = getattr(module, name)
            if isclass(item) and issubclass(item, self._class):
                reg_name = self._name_getter(item)
                if reg_name is not None:
                    self._classes[reg_name] = item

    def get_class(self, name: str):
        return self._classes.get(name)

    def __str__(self) -> str:
        lines = ["Generic Loader. Loaded stuff:"]

        for recipe_name, recipe_class in self._classes.items():
            lines.append(f" - {recipe_name} from {recipe_class.__module__}")

        return "\n".join(lines)
