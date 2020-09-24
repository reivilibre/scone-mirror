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
