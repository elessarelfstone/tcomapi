import argparse
import datetime
import ipaddress
import os


def check_fpath(value):
    if not os.path.exists(value):
        raise argparse.ArgumentTypeError(
            f"{value} doesn't exists")
    return value


def check_ip_port(value):
    def fail_address_port_format():
        raise argparse.ArgumentTypeError(
            f"{value} is not a correct address:port pair")

    try:
        addr, port = value.split(':')
        ip = ipaddress.ip_address(addr)
        port = int(port)
        if not 1 <= port <= 65535:
            raise ValueError('Wrong port')
    except ValueError:
        raise fail_address_port_format()

    return addr + ':' + str(port)


def check_positive_float(value):
    def fail():
        raise argparse.ArgumentTypeError(
            f"{value} is not a valid value")
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
            f"{value} is not a valid value")
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
            f"{value} is not a valid date range")
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


def common_corrector(value):
    return value.rstrip().replace('"', "'").replace('\n', '')


def date_corrector(value):
    return common_corrector(value).split('+')[0]
