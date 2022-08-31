import os

from pathlib import Path

from config import DATA_FOLDER


__all__ = ["DATA_PATH"]


DATA_PATH = Path(DATA_FOLDER) / "api"
os.makedirs(DATA_PATH, exist_ok=True)
