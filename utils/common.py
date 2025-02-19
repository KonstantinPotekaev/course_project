import argparse
from pathlib import Path
from typing import Iterable, Generator, Any

import yaml

import utils.exceptions as ut_exc

BOOL_TRUE_STRINGS = ("yes", "true", "t", "y", "1")
BOOL_FALSE_STRINGS = ("no", "false", "f", "n", "0")


def load_config(conf_path: str) -> dict:
    conf = Path(conf_path)
    if not conf.exists():
        raise ut_exc.ConfigException(f"Config file '{conf_path}' was not found")

    with open(conf) as conf_file:
        return yaml.safe_load(conf_file)


def grouper(iterable: Iterable, n: int) -> Generator:
    """
    Разбивает iterable на группы по n элементов,
    последнюю группу делает меньше n
    """
    for i in range(0, len(iterable), n):
        yield iterable[i: i + n]


def str_to_bool(bool_str: str) -> bool:
    if bool_str.lower() in BOOL_TRUE_STRINGS:
        return True
    elif bool_str.lower() in BOOL_FALSE_STRINGS:
        return False
    else:
        raise argparse.ArgumentTypeError(
            f"Boolean value expected. Use for 'True': {BOOL_TRUE_STRINGS}."
            f" For 'False': {BOOL_FALSE_STRINGS}"
        )


def parse_bool(var: Any):
    if isinstance(var, str):
        return str_to_bool(var)
    if isinstance(var, bool) or isinstance(var, int):
        return bool(var)
    raise ValueError(f"Parsing type '{type(var)}' to bool is not supported")
