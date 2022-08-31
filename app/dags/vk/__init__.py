import os

from .reader import VKReader
from .writer import VKWriter

from .. import DATA_PATH


__all__ = ["reader", "writer"]


DATA = DATA_PATH / "vk"
os.makedirs(DATA, exist_ok=True)

reader = VKReader(DATA)
writer = VKWriter(DATA)
