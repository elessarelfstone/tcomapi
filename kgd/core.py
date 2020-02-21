import os
import sys
from collections import deque, namedtuple
from enum import Enum
from time import sleep
from typing import NamedTuple

from box import Box
from requests import HTTPError, ConnectionError
from tqdm import tqdm

from kgd.exceptions import KgdTooManyRequests
from kgd.cli import parse_args
from kgd.constants import (PROCESSED_EXISTS_MESSAGE,
                           PROCESSED_NOT_EXISTS_MESSAGE, ExitStatus, HOST)
from kgd.parsing import TaxPaymentParser, processed_bins_fpath
from kgd.utils import load_lines, append_file, is_server_up


class TqdmUpTo(tqdm):
    def update_to(self, filename, bn, fails, reproc=False):
        if reproc:
            caption = 'Reprocessing {} in {} ... Fails: {}'
            cnt = 0
        else:
            caption = 'Processing {} in {} ... Fails: {}'
            cnt = 1

        self.set_description(caption.format(bn, filename, fails))
        self.update(cnt)


def main():
    exit_status = ExitStatus.SUCCESS
    args = parse_args()
    p = Box(vars(args))
    prsd_bins = []

    timeout = p.timeout
    del p["timeout"]

    if not is_server_up(HOST):
        return ExitStatus.ERROR_SERVER_IS_DOWN

    # if file with parsed BINS exists
    # that means we've processed some part of them before
    # also it might be all of them...
    # but with fails(connection, network)
    if not os.path.exists(processed_bins_fpath(p.fpath)):
        print(PROCESSED_NOT_EXISTS_MESSAGE)
        open(processed_bins_fpath(p.fpath), 'a').close()
    else:
        print(PROCESSED_EXISTS_MESSAGE)
        prsd_bins = load_lines(processed_bins_fpath(p.fpath))

    src_bins = load_lines(p.fpath)

    # all BINS minus parsed
    if prsd_bins:
        s = set(src_bins)
        s.difference_update(set(prsd_bins))
        bins = list(s)
    else:
        bins = src_bins

    # no BINS to process
    if not bins:
        return ExitStatus.ERROR_NO_BINS

    failed_bins = deque()
    bins = deque(bins)

    fatal_cnt = 0
    big_timeout = timeout

    total = len(bins)
    parser = TaxPaymentParser(p.fpath, p.fsize)
    with TqdmUpTo(total=total) as pbar:
        is_fail = False
        while bins:
            try:
                # get new BIN if last time we failed
                if is_fail:
                    bn = bins.popleft()
                    reproc = False
                else:
                    # otherwise we can reprocess
                    # old failed BIN
                    if parser.fails:
                        bn = parser.pop_failed()
                        reproc = True
                    else:
                        # no fails, go with new BIN
                        bn = bins.popleft()
                        reproc = False

                fname = os.path.basename(parser.output_file)

                parser.process(bn, **p,
                               hook=pbar.update_to(fname,
                                                   bn,
                                                   len(parser.fails),
                                                   reproc=reproc))
            except (HTTPError, ConnectionError) as e:
                sleep(timeout)
                if is_server_up(HOST):
                    parser.put_failed(bn)
                else:
                    print(e)
                    return ExitStatus.ERROR
                # fatal_cnt += 1
                # big_timeout += timeout
                # if fatal_cnt > 30:
                #     print(e)
                #     return ExitStatus.ERROR
                # sleep(big_timeout)

            except KgdTooManyRequests:
                parser.put_failed(bn)
                is_fail = True
                # exit if it's too many fails
                fails_percent = len(parser.fails) * 100 / total
                if fails_percent > 90:
                    return ExitStatus.ERROR
                sleep(timeout)

            else:
                is_fail = False

                # successfully processed BINs will be excluded next time
                append_file(processed_bins_fpath(p.fpath), bn)

    return ExitStatus.SUCCESS
