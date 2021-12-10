from datetime import datetime

from tcomapi.common.utils import fname_noext


def last_file_with_bins(flist):
    dates = []
    for f in flist:
        fname = fname_noext(f)
        _dt = fname.split('.')[0].split('_')[3]
        dt = datetime.strptime(_dt, '%Y-%m-%d')
        dates.append((dt, f))

    dates.sort(key=lambda x: x[1])

    return dates[-1][1]

