import os

from app.dags import DATA_PATH

from .reader import VKReader
from .writer import VKWriter


__all__ = ["reader", "writer"]


DATA = DATA_PATH / "vk"
os.makedirs(DATA, exist_ok=True)

reader = VKReader(DATA)
writer = VKWriter(DATA)
