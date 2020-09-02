from calendar import monthrange
from datetime import datetime, date, timedelta
from typing import Tuple

from utils import prev_month

FILENAME_DATE_FORMAT = '%Y%m%d'
PERIOD_DATE_FORMAT = '%Y-%m-%d'
PERIOD_MONTH_FORMAT = '%Y-%m'


def first_dayof_month(dt: date) -> date:
    return dt.replace(day=1)


def last_dayof_month(dt: date) -> date:
    return date(dt.year, dt.month, monthrange(dt.year, dt.month)[1])


def first_dayof_month_as_str(dt: date, dt_format=FILENAME_DATE_FORMAT) -> str:
    return dt.replace(day=1).strftime(dt_format)


def today_as_str(dt_format=FILENAME_DATE_FORMAT) -> str:
    return datetime.today().strftime(dt_format)


def month_as_period(month: str) -> Tuple[date, date]:
    _date = datetime.strptime(month, PERIOD_MONTH_FORMAT)
    return first_dayof_month(_date), last_dayof_month(_date)


def previous_month_as_period(month: str) -> Tuple[date, date]:
    _date = datetime.strptime(month, PERIOD_MONTH_FORMAT) - timedelta(days=1)
    return first_dayof_month(_date), _date


def previous_month_as_period_str(month: str) -> Tuple[str, str]:
    _date = datetime.strptime(month, PERIOD_MONTH_FORMAT) - timedelta(days=1)
    return first_dayof_month_as_str(_date), _date.strftime(PERIOD_DATE_FORMAT)


def month_as_dates_range(month, frmt):
    year = int(month[:4])
    month = int(month[-2:])
    start_date = date(year, month, 1).strftime(frmt)
    end_date = date(year, month, monthrange(year, month)[1]).strftime(frmt)
    return start_date, end_date


def default_month() -> str:
    """ Return  """
    today = date.today()
    year, month = prev_month(today.year, today.month)
    return '{}-{:02}'.format(year, month)