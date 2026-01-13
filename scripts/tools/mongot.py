import os
from os import PathLike
from pathlib import Path

MONGOT_ROOT = Path(os.path.dirname(os.path.realpath(__file__))).parent.parent


def path_in_repo(path) -> PathLike:
    return MONGOT_ROOT / path
