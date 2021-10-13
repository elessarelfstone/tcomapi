import enum
from calendar import monthrange
from datetime import datetime, date, timedelta
from typing import Tuple

FILENAME_DATE_FORMAT = '%Y%m%d'
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
PERIOD_MONTH_FORMAT = '%Y-%m'


class LastPeriod(enum.Enum):
    a = -1
    t = 0
    ld = 1
    lw = 2
    lm = 3

    @classmethod
    def get_period(cls, val):
        if val == cls.t:
            # t = today_as_str(DEFAULT_DATE_FORMAT)
            t = datetime.today()
            return t, t
        # TODO implement other periods
        else:
            return None


def first_dayof_month(dt: date) -> date:
    return dt.replace(day=1)


def last_dayof_month(dt: date) -> date:
    return date(dt.year, dt.month, monthrange(dt.year, dt.month)[1])


def first_dayof_month_as_str(dt: date, dt_format=FILENAME_DATE_FORMAT) -> str:
    return dt.replace(day=1).strftime(dt_format)


def today_as_str(dt_format=FILENAME_DATE_FORMAT) -> str:
    return datetime.today().strftime(dt_format)


def yesterday_as_str(dt_format=FILENAME_DATE_FORMAT) -> str:
    """ """
    yesterday = datetime.today() - timedelta(days=1)
    return yesterday.strftime(dt_format)


def month_as_range(month: str) -> Tuple[date, date]:
    _date = datetime.strptime(month, PERIOD_MONTH_FORMAT)
    return first_dayof_month(_date), last_dayof_month(_date)


def previous_month_as_range(month: str) -> Tuple[date, date]:
    _date = datetime.strptime(month, PERIOD_MONTH_FORMAT) - timedelta(days=1)
    return first_dayof_month(_date), _date


def previous_month_as_range_str(month: str) -> Tuple[str, str]:
    _date = datetime.strptime(month, PERIOD_MONTH_FORMAT) - timedelta(days=1)
    return first_dayof_month_as_str(_date), _date.strftime(DEFAULT_DATE_FORMAT)


def month_as_dates_range(month, frmt):
    year = int(month[:4])
    month = int(month[-2:])
    start_date = date(year, month, 1).strftime(frmt)
    end_date = date(year, month, monthrange(year, month)[1]).strftime(frmt)
    return start_date, end_date


def previous_month(year, month) -> Tuple[int, int]:
    """ According given year and month return previous month as tuple (year, month)"""
    first_day = date(year, month, 1)
    lastday_prevmonth = first_day - timedelta(days=1)
    return lastday_prevmonth.year, lastday_prevmonth.month


def previous_month_as_str() -> str:
    """ Return string representing privious month formatted as YYYY-MM """
    today = date.today()
    year, month = previous_month(today.year, today.month)
    return '{}-{:02}'.format(year, month)


def previous_date_as_str(delta, frmt=DEFAULT_DATE_FORMAT):
    t = datetime.today() - timedelta(delta)
    return t.strftime(frmt)
