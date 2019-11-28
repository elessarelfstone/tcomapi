import argparse
import datetime
import os


def check_fpath(value):
    if not os.path.exists(value):
        raise argparse.ArgumentTypeError(
            "%s doesn't exists" % value)
    return value


def check_positive_float(value):
    def fail():
        raise argparse.ArgumentTypeError(
            "%s is not a valid value" % value)
    try:
        fvalue = float(value)
    except ValueError:
        fail()
    if fvalue <= 0:
        fail()
    return fvalue


def check_positive_int(value):
    def fail():
        raise argparse.ArgumentTypeError(
            "%s is not a valid value" % value)
    try:
        fvalue = int(value)
    except ValueError:
        fail()
    if fvalue <= 0:
        fail()
    return fvalue


def check_date_range(value):
    def fail():
        raise argparse.ArgumentTypeError(
            "%s is not a valid date range" % value)
    try:
        date_range = value.split(':')
        if len(date_range) != 2:
            raise ValueError('Date range error')

        begin_dt = datetime.datetime.strptime(date_range[0], '%Y-%m-%d')
        end_dt = datetime.datetime.strptime(date_range[1], '%Y-%m-%d')
        if begin_dt > end_dt:
            fail()

        fvalue = date_range[0], date_range[1]
    except ValueError:
        fail()

    return fvalue


def strip_converter(value):
    return value.rstrip().replace('"', "'").replace('\n', '')


def date_converter(value):
    return strip_converter(value).split('+')[0]
