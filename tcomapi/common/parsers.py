from os.path import dirname, exists, getsize
from collections import deque

from tcomapi.common.utils import build_fpath, read_lines


def check_id(_id):
    if len(_id) != 12:
        return False
    else:
        return all([c.isdigit() for c in _id])


class BidsBigDataToCsvHandler:

    parsed_file_ext = 'prs'
    success_file_ext = 'success'

    def __init__(self, name, bids_fpath, limit_outputfsize=None, ext='csv'):

        self._out_fpaths = []
        self._name = name
        self._bids_fpath = bids_fpath
        self._limit_outputfsize = limit_outputfsize
        self._ext = ext

        self._failed_bids = deque([])

        # file path with already parsed bids
        self._parsed_fpath = build_fpath(dirname(bids_fpath), self._name, self.parsed_file_ext)

        # success file path...usually store statistic data
        self._success_fpath = build_fpath(dirname(bids_fpath), self._name, self.success_file_ext)

        out_fpath = build_fpath(dirname(bids_fpath), self._name, self._ext)

        # collect all existed data csv files paths
        if self._limit_outputfsize:
            self._output = []
            num = 1
            while exists(out_fpath):
                self._output.append(out_fpath)
                num += 1
                out_fpath = build_fpath(dirname(bids_fpath), f'{self._name}_{num}', 'prs')

            if not self._output:
                open(out_fpath, 'a').close()
                self._output.append(out_fpath)
        else:
            self._output = out_fpath

        # bids loading
        parsed_bids = []
        if exists(self._parsed_fpath):
            parsed_bids = read_lines(self._parsed_fpath)

        self._parsed_bids_count = len(parsed_bids)

        source_bids = [bid for bid in read_lines(bids_fpath) if check_id(bid)]
        self._source_bids_count = len(source_bids)

        # exclude parsed
        if parsed_bids:
            s = set(source_bids)
            s.difference_update(set(parsed_bids))
            self._bids = deque(list(s))
        else:
            self._bids = deque(source_bids)

    def _add_output(self):
        """ Add new output file"""
        num = len(self._output)
        new_output = build_fpath(dirname(self._bids_fpath),
                                 f'{self._name}_{num}', self._ext)
        open(new_output, 'a').close()
        self._output.append(new_output)

    @property
    def bids(self):
        return self._bids

    @property
    def output(self):
        if isinstance(self._output, list):
            size = getsize(self.output[-1])
            if size >= self._limit_outputfsize:
                self._add_output()
            return self._output[-1]

        return self._output

    @property
    def parsed_fpath(self):
        return self._parsed_fpath

    @property
    def failed_bids(self):
        return self._failed_bids

    @property
    def source_bids_count(self):
        return self._source_bids_count

    @property
    def parsed_bids_count(self):
        return self._parsed_bids_count

    @property
    def success_fpath(self):
        return self._success_fpath


