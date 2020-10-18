from copy import deepcopy
from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional

ExpressionPart = NamedTuple("ExpressionPart", [("kind", str), ("value", str)])


def flatten_dict(nested: Dict[str, Any]) -> Dict[str, Any]:
    for key in nested:
        if not isinstance(key, str):
            # not possible to flatten
            return nested

    flat = {}

    for key, value in nested.items():
        if isinstance(value, dict) and value:
            sub_flat = flatten_dict(value)
            for k in sub_flat:
                if not isinstance(k, str):
                    flat[key] = value
                    break
            else:
                # can flatten
                for k, v in sub_flat.items():
                    flat[f"{key}.{k}"] = v
        else:
            flat[key] = value

    return flat


class ExprParsingState(Enum):
    NORMAL = 1
    DOLLAR = 2
    VARIABLE_NAME = 3


def parse_expr(expr: str) -> List[ExpressionPart]:
    state = ExprParsingState.NORMAL
    buffer = ""
    out = []
    for char in expr:
        if state == ExprParsingState.NORMAL:
            if char == "$":
                state = ExprParsingState.DOLLAR
            else:
                buffer += char
        elif state == ExprParsingState.DOLLAR:
            if char == "$":
                # escaped dollar sign
                buffer += "$"
                state = ExprParsingState.NORMAL
            elif char == "{":
                state = ExprParsingState.VARIABLE_NAME
                if buffer:
                    out.append(ExpressionPart("literal", buffer))
                    buffer = ""
            else:
                buffer += "$" + char
                state = ExprParsingState.NORMAL
        elif state == ExprParsingState.VARIABLE_NAME:
            if char == "}":
                state = ExprParsingState.NORMAL
                out.append(ExpressionPart("variable", buffer))
                buffer = ""
            else:
                buffer += char

    if state != ExprParsingState.NORMAL:
        raise ValueError(f"Wrong end state: {state}")

    if buffer:
        out.append(ExpressionPart("literal", buffer))

    return out


def merge_right_into_left_inplace(left: dict, right: dict):
    for key, value in right.items():
        if isinstance(value, dict) and key in left and isinstance(left[key], dict):
            merge_right_into_left_inplace(left[key], value)
        else:
            left[key] = value


class Variables:
    def __init__(self, delegate: Optional["Variables"]):
        self._vars: Dict[str, Any] = {}
        self._delegate: Optional[Variables] = delegate

    def get_dotted(self, name: str) -> Any:
        current = self._vars
        keys = name.split(".")
        try:
            for k in keys:
                current = current[k]
            return current
        except KeyError:
            if self._delegate:
                return self._delegate.get_dotted(name)
            raise KeyError("No variable: " + name)

    def has_dotted(self, name: str) -> bool:
        try:
            self.get_dotted(name)
            return True
        except KeyError:
            if self._delegate:
                return self._delegate.has_dotted(name)
            return False

    def set_dotted(self, name: str, value: Any):
        current = self._vars
        keys = name.split(".")
        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]
        current[keys[-1]] = value

    def _eval_with_incoming(self, expr: str, incoming: Dict[str, str]) -> Any:
        parsed = parse_expr(expr)

        if len(parsed) == 1 and parsed[0].kind == "variable":
            var_name = parsed[0].value
            if self.has_dotted(var_name):
                return self.get_dotted(var_name)
            elif var_name in incoming:
                sub_expr = incoming.pop(var_name)
                sub_val = self._eval_with_incoming(sub_expr, incoming)
                self.set_dotted(var_name, sub_val)
                return sub_val
            else:
                raise KeyError(f"No variable '{var_name}'")

        out = ""
        for part in parsed:
            if part.kind == "literal":
                out += part.value
            elif part.kind == "variable":
                var_name = part.value
                if self.has_dotted(var_name):
                    out += str(self.get_dotted(var_name))
                elif var_name in incoming:
                    sub_expr = incoming.pop(var_name)
                    sub_val = self._eval_with_incoming(sub_expr, incoming)
                    self.set_dotted(var_name, sub_val)
                    out += str(sub_val)
                else:
                    raise KeyError(f"No variable '{var_name}'")
        return out

    def load_vars_with_substitutions(self, incoming: Dict[str, Any]):
        incoming = flatten_dict(incoming)
        while incoming:
            key, expr = incoming.popitem()
            value = self._eval_with_incoming(expr, incoming)
            self.set_dotted(key, value)

    def eval(self, expr: str) -> Any:
        return self._eval_with_incoming(expr, {})

    def load_plain(self, incoming: Dict[str, Any]):
        merge_right_into_left_inplace(self._vars, incoming)

    def substitute_inplace_in_dict(self, dictionary: Dict[str, Any]):
        for k, v in dictionary.items():
            if isinstance(v, dict):
                self.substitute_inplace_in_dict(v)
            elif isinstance(v, str):
                dictionary[k] = self.eval(v)

    def substitute_in_dict_copy(self, dictionary: Dict[str, Any]):
        new_dict = deepcopy(dictionary)
        self.substitute_inplace_in_dict(new_dict)
        return new_dict

    def toplevel(self):
        return self._vars
