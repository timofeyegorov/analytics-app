from importlib import import_module

from . import utils


def calculate_tables(func: str, *args, **kwargs):
    module = import_module(f"{__name__}._calculate_tables")
    return getattr(module, func)(*args, **kwargs)
