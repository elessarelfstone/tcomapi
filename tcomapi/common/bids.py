import os
from os.path import dirname, exists, getsize
from collections import deque

from tcomapi.common.utils import build_fpath, read_lines


def check_id(_id):
    if len(_id) != 12:
        return False
    else:
        return all([c.isdigit() for c in _id])


class BidsHandler:
    """ bid - business id"""
    def __init__(self, bids_fpath, output_fpath, parsed_fpath):
        self.failed_bids = deque([])
        self.output_fpath = output_fpath

        parsed_bids = []
        if exists(parsed_fpath):
            parsed_bids = read_lines(parsed_fpath)

        self._parsed_bids_count = len(parsed_bids)

        source_bids = [bid for bid in read_lines(bids_fpath) if check_id(bid)]
        self._source_bids_count = len(source_bids)

        # excluding parsed bids
        if parsed_bids:
            s = set(source_bids)
            s.difference_update(set(parsed_bids))
            self._bids = deque(list(s))
        else:
            self._bids = deque(source_bids)

    @property
    def source_bids_count(self):
        return self._source_bids_count

    @property
    def parsed_bids_count(self):
        return self._parsed_bids_count

    @property
    def bids(self):
        return self._bids

    def status_info(self, bid, is_reprocess=False, stat=None):
        r = 'Reprocessing' if is_reprocess else ''
        s = f'Total: {self.source_bids_count}. Parsed: {self.parsed_bids_count}. ' + os.linesep
        s += f'Parsing payments on {bid} in {self.output_fpath}. {r}' + os.linesep
        s += stat
        return s