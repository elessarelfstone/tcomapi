import attr
import os
from dataclasses import dataclass

from tcomapi.common.utils import load_lines, get_base_fpath


@attr.s
class FileInfo:
    fpath = attr.ib(default='')
    size = attr.ib(default='')
    num = attr.ib(default='')


# @dataclass
# class FileInfo:
#     fpath: str
#     size: int
#     num: int


def check_id(_id):
    if len(_id) != 12:
        return False
    else:
        return all([c.isdigit() for c in _id])


class DataFileHelper:

    """ Helper for handling all
        input and output files manipulations
    """
    def __init__(self, ids_fpath, limit_fsize=1000000, ext='csv'):
        self._output_fpaths = []
        self._ids_fpath = ids_fpath

        self._limit_fsize = limit_fsize
        self._ext = ext
        # self._ids = self.load_ids()
        # self._all_cnt = len(self._ids)
        self._all_cnt = 0
        self._prs_cnt = 0

        # file dirname and file name without extension
        self._fpath_base = get_base_fpath(ids_fpath)

        # file path for logging parsed ids
        self._parsed_fpath = f'{self._fpath_base}.prs'

        # gather all existsing out files
        self._prepare()

    @property
    def all_count(self):
        return self._all_cnt

    @property
    def prs_count(self):
        return self._prs_cnt

    def _prepare(self):

        # first out file
        output_fpath = f'{self._fpath_base}_out.{self._ext}'
        num = 2

        # gather all existed out files
        while os.path.exists(output_fpath):
            f_info = FileInfo(fpath=output_fpath,
                              size=os.path.getsize(output_fpath),
                              num=num - 1)
            self._output_fpaths.append(f_info)
            output_fpath = f'{self._fpath_base}_out_{num}.{self._ext}'
            num += 1

        # if we haven't created any out file
        # initialize first out file
        if not self._output_fpaths:
            open(output_fpath, 'a').close()
            f_info = FileInfo(fpath=output_fpath,
                              size=os.path.getsize(output_fpath),
                              num=num - 1)
            self._output_fpaths.append(f_info)

    def _curr(self):

        # current out file will always
        # be last item of _output_fpaths
        return self._output_fpaths[-1]

    @property
    def size(self):
        return os.path.getsize(self.curr_file)

    @property
    def curr_file(self):
        size = os.path.getsize(self._curr().fpath)
        if size >= self._limit_fsize:
            return self.update()
        return self._curr().fpath

    @property
    def parsed_file(self):
        return self._parsed_fpath

    @property
    def src_ids(self):
        return load_lines(self._ids_fpath)

    @property
    def prs_ids(self):
        return load_lines(self.parsed_file)

    def update(self):

        # increase number suffix for filename
        num = self._curr().num + 1

        # build filename
        _new_fpath = f'{self._fpath_base}_out_{num}.{self._ext}'

        # create empty out file
        open(_new_fpath, 'a').close()

        # initialization info for new out file
        f_info = FileInfo(fpath=_new_fpath, size=0, num=num)
        self._output_fpaths.append(f_info)

        return self._curr().fpath

    def load_ids(self):
        _ids = []

        # if there are parsed IDs
        if os.path.exists(self._parsed_fpath):
            _ids = self.prs_ids
            self._prs_cnt = len(_ids)

        # check each ID, it could be trash
        ids = [_id for _id in self.src_ids if check_id(_id)]
        self._all_cnt = len(ids)

        # exclude parsed
        if _ids:
            s = set(ids)
            s.difference_update(set(_ids))
            return list(s)
        else:
            return ids