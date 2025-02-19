import json
from functools import partial
from itertools import islice
from logging import getLogger
from pathlib import Path
from typing import Iterable

logger = getLogger(__name__)


def iter_grouper(segment_gen: Iterable, n: int) -> Iterable:
    """
    Разбивает iterable на группы по n элементов,
    последнюю группу делает меньше n
    """

    def _get_n(seq: Iterable, group_size: int) -> list:
        return list(islice(seq, group_size))

    return iter(partial(_get_n, iter(segment_gen), n), [])


class Statistics:
    FILES = "files"
    TIME = "time"
    STATISTICS = []

    @staticmethod
    def add_statistic(proc_files: list, proc_time: float):
        Statistics.STATISTICS.append({
            Statistics.FILES: proc_files,
            Statistics.TIME: f"{proc_time:.2f}"
        })

    @staticmethod
    def save_statistics(out_dir: Path):
        out_path = Path(out_dir, 'stats.json')
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, 'w', encoding="utf-8") as fp:
            json.dump(Statistics.STATISTICS, fp, indent=2)
