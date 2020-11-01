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

from typing import Any, Optional, Type, TypeVar, cast

from typeguard import check_type as check_typeguard

_A = TypeVar("_A")


def check_type(value: Any, check_type: Type[_A], name: str = "value") -> _A:
    check_typeguard(name, value, check_type)
    # if not isinstance(value, check_type):
    #    raise TypeError(f"Not of type {check_type}")
    return cast(_A, value)


def check_type_opt(
    value: Any, check_type: Type[_A], name: str = "value"
) -> Optional[_A]:
    check_typeguard(name, value, Optional[check_type])
    # if not isinstance(value, check_type):
    #    raise TypeError(f"Not of type {check_type}")
    return cast(_A, value)


def check_type_adv(value: Any, check_type: Any, name: str = "value") -> Any:
    # permitted to use special forms with this one
    check_typeguard(name, value, check_type)
    return value
