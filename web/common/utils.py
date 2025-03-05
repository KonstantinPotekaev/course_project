import asyncio
from functools import partial
from itertools import islice
from logging import getLogger
from typing import Iterable

import nest_asyncio

logger = getLogger(__name__)
nest_asyncio.apply()


def iter_grouper(segment_gen: Iterable, n: int) -> Iterable:
    """
    Разбивает iterable на группы по n элементов,
    последнюю группу делает меньше n
    """

    def _get_n(seq: Iterable, group_size: int) -> list:
        return list(islice(seq, group_size))

    return iter(partial(_get_n, iter(segment_gen), n), [])


def run_async(coroutine):
    """
    Запускает асинхронную корутину и возвращает результат.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    res = loop.run_until_complete(coroutine)
    return res
