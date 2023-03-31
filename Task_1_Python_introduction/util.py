"""
helper functions
"""


def is_callable(obj: object, attr_name: str) -> bool:
    return callable(getattr(obj, attr_name))


def is_dunder(attr_name: str) -> bool:
    return attr_name.startswith("__")


def is_user_property(obj: object, attr_name: str) -> bool:
    """
    checks if attribute is user defined self.property or @property
    """
    return not is_callable(obj, attr_name) and not is_dunder(attr_name)


def attr_string(obj: object, attr_name: str) -> str:
    """
    return string: "attribute_name: attribute_value"
    """
    return f"{attr_name}: {(getattr(obj, attr_name))}"


def object_attrs(obj: object) -> list[str]:
    """
    return list of strings : ["attribute_name: attribute_value", ...]
    """
    return [attr_string(obj, attr) for attr in dir(obj) if is_user_property(obj, attr)]
