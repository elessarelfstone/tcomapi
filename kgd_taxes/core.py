import os
import sys

from box import Box
from tqdm import tqdm

from exceptions import NetworkError
from cli import parse_args
from constants import (PROCESSED_EXISTS_MESSAGE, PROCESSED_NOT_EXISTS_MESSAGE,
                       ExitStatus)
from parsing import TaxPaymentParser, processed_bins_fpath
from utils import load_lines, append_file


class TqdmUpTo(tqdm):
    def update_to(self, filename, bn, fails):
        self.set_description('Processing {} in {} ... Fails: {}'.format(bn, filename, fails))
        self.update(1)


def main():
    exit_status = ExitStatus.SUCCESS
    args = parse_args()
    p = Box(vars(args))
    prsd_bins = []

    if not os.path.exists(processed_bins_fpath(p.fpath)):
        print(PROCESSED_NOT_EXISTS_MESSAGE)
        open(processed_bins_fpath(p.fpath), 'a').close()
    else:
        print(PROCESSED_EXISTS_MESSAGE)
        prsd_bins = load_lines(processed_bins_fpath(p.fpath))

    src_bins = load_lines(p.fpath)
    if prsd_bins:
        bins = [x for x in src_bins if x not in prsd_bins]
    else:
        bins = src_bins

    if not bins:
        return ExitStatus.ERROR_NO_BINS

    parser = TaxPaymentParser(p.fpath, p.fsize)
    with TqdmUpTo(total=len(bins)) as pbar:
        for bn in bins:
            # p.bn = bn
            try:
                # tax_parser.process(p.address_port, p.token, bn, p.fpath, p.fsize,
                #                    p.date_range, p.retries, p.backoff, p.timeout,
                #                    reporthook=pbar.update_to(os.path.basename(tax_parser.output_file),
                #                                              bn, tax_parser.fails))
                fname = os.path.basename(parser.output_file)
                parser.process(bn, **p, reporthook=pbar.update_to(fname, bn, parser.fails))

            except NetworkError:
                # we need break if it's too many fails
                fails_percent = parser.fails * 100 / len(bins)
                if fails_percent > 90:
                    return ExitStatus.ERROR
            else:
                # successfully processed BINs will be excluded next time
                append_file(processed_bins_fpath(p.fpath), bn)

    return ExitStatus.SUCCESS
