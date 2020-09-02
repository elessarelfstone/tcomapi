import pytest
from datetime import date

from tcomapi.common.dates import first_dayof_month, last_dayof_month


@pytest.mark.parametrize("dt, first_day", [(date(2020, 6, 14), date(2020, 6, 1)),
                                           (date(2020, 2, 28), date(2020, 2, 1))])
def test_first_dayof_month(dt, first_day):
    assert first_dayof_month(dt) == first_day


@pytest.mark.parametrize("dt, last_day", [(date(2020, 2, 14), date(2020, 2, 29)),
                                          (date(2019, 2, 3), date(2019, 2, 28)),
                                          (date(2019, 12, 30), date(2019, 12, 31)),
                                          (date(2019, 6, 1), date(2019, 6, 30))])
def test_last_dayof_month(dt, last_day):
    assert last_dayof_month(dt) == last_day



def test_previous_month_as_period():
    pass