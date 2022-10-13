import os

from app.dags import DATA_PATH

from .reader import RoistatReader
from .writer import RoistatWriter


__all__ = ["reader", "writer"]


DATA = DATA_PATH / "roistat"
os.makedirs(DATA, exist_ok=True)

reader = RoistatReader(DATA)
writer = RoistatWriter(DATA)
