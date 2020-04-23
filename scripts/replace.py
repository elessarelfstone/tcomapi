import fnmatch
import gzip
import os
import re
import platform
import shutil
import sys
from datetime import datetime
from os.path import isfile, join
from time import sleep

dt_format = "%Y%m%d"


def creation_date(fpath):
    return datetime.fromtimestamp(os.path.getmtime(fpath))


def fnames(folder):
    full_names = [f for f in os.listdir(folder) if isfile(join(folder, f))]
    # only prefix e.g. statgov_companies, not statgov_companies_20200420.csv.gzip
    only_prefixes = set(['_'.join(fn.split('_')[:-1]) for fn in full_names])
    return full_names, only_prefixes


def main(fsdir, size, ext=None):
    # get full names of files
    # and their prefixes(names without extensions and dates)
    fns, prxs = fnames(fsdir)
    _today = datetime.today().strftime(dt_format)

    for i, f_pref in enumerate(prxs):

        # for each prefix we search one or two last files
        # filter by prefix, for example - statgov_companies
        _fns = fnmatch.filter(fns, '{}*'.format(f_pref))
        if ext:
            # if extension is given
            _fns = fnmatch.filter(fns, '*.{}'.format(ext))

        # get path for each file
        fpths = [os.path.join(fsdir, fn) for fn in _fns]
        # filter only appropriate sized files
        _well_sized = [f for f in fpths if os.path.getsize(f) > int(size)]

        src_fpath = None
        if _well_sized:
            src_fpath = sorted(_well_sized, key=lambda x: creation_date(x))[-1]

        if src_fpath and creation_date(src_fpath).date() == datetime.today().date():
            continue

        if src_fpath:
            m = re.search("([0-9]{4}[0-9]{2}[0-9]{2})", src_fpath)
            if m:
                dst_path = src_fpath.replace(m.group(0), '{}').format(_today)
                shutil.copy(src_fpath, dst_path)
                print('{} > {}'.format(os.path.basename(src_fpath), os.path.basename(dst_path)))
        # sleep(3)
        # percent = '{}%'.format(round((i * 100) / len(prxs)))
        # print(percent)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
