import os

from pathlib import Path
from importlib import import_module


bps = []

commands_path = Path(__file__).parent
for item in os.listdir(commands_path):
    file_path = Path(item)
    if file_path.name.startswith("__") or not (commands_path / file_path).is_file():
        continue
    module = import_module(f"{__name__}.{file_path.stem}")
    try:
        bps.append(module.bp)
    except AttributeError:
        pass
