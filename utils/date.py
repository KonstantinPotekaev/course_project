import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from utils.exceptions import WrongDateFormatException

# Datetime in ISO 8601 format
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
# то же самое для кликхауза начиная с версии 23.4
DATE_FORMAT_CH = "%Y-%m-%dT%T"
DAY_FORMAT = "%Y-%m-%d"
DATE_FORMAT_T_Z = "%Y-%m-%dT%H:%M:%SZ"
DATE_FORMAT_SPACE = "%Y-%m-%d %H:%M:%S"


TIME_UNITS = {
    "m": timedelta(minutes=1),
    "h": timedelta(hours=1),
    "d": timedelta(days=1),
    "w": timedelta(weeks=1),
    "M": timedelta(days=30),
    "y": timedelta(days=365),
}

INTERVAL_EXPR = re.compile("(\d+)([mhdwMy])")


def parse_interval(interval: str) -> Optional[timedelta]:
    if not interval:
        # по умолчанию возвращаем 1 день
        return timedelta(days=1)
    match_res = INTERVAL_EXPR.match(interval)
    if match_res:
        return TIME_UNITS[match_res[2]] * int(match_res[1])


def iso_to_date_str(date: str) -> str:
    return date.split("T")[0]


def iso_to_date(date: str) -> datetime.date:
    return datetime.strptime(date, DATE_FORMAT).date()


def iso_to_datetime(date: str) -> datetime:
    return datetime.strptime(date, DATE_FORMAT)


def iso_to_timestamp(date: str) -> float:
    return iso_to_datetime(date).timestamp()


def str_to_datetime(date: str, format_: str) -> datetime:
    dt = datetime.strptime(date, format_)
    return dt


def mark_as_utc(date: datetime) -> datetime:
    return date.replace(tzinfo=timezone.utc)


def check_date_format(
    date: str, format_str: str, raise_exc: bool = False
) -> Optional[bool]:
    try:
        datetime.strptime(date, format_str)
    except ValueError:
        if raise_exc:
            raise WrongDateFormatException(
                f"Expected date format: '{format_str}'," f" but '{date}' received"
            )
        else:
            return False
    return True
