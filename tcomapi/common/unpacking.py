import os
from fnmatch import fnmatch
from os.path import basename, join
from shutil import which, move
from zipfile import ZipFile
from rarfile import RarFile

from tcomapi.common.exceptions import ExternalSourceError
from tcomapi.common.utils import fname_noext, build_fpath, identify_format


class UnpackingError(Exception):
    pass


def determine_unrar():
    """
    Determine if unrar is installed
    """
    if which('unrar'):
        return 'unrar'

    return None


def fflist(apath, frmt):

    fnames = []

    if frmt == "rar":
        fnames = raro_flist(apath)
    elif frmt == "zip":
        fnames = zipo_flist(apath)
    else:
        raise UnpackingError('Unsupported archive type')

    return fnames


def unpack(apath, fpaths):
    """ Unpack files from archive. Support only rar, zip archives """

    frmt = identify_format(apath)

    if not frmt:
        raise ExternalSourceError("Unknown format of file.")

    # rename apath name with its format
    # cause before we have file without extension
    folder = os.path.abspath(os.path.dirname(apath))
    bsname = os.path.basename(apath)
    _apath = os.path.join(folder, f'{bsname}.{frmt}')
    move(apath, _apath)

    if frmt:
        if frmt == 'zip':
            # get list of all files in zip and Zip object
            archo, flist = zipo_flist(_apath)
        elif frmt == 'rar':

            # check if unrar installed
            if determine_unrar():

                # get list of all files in rar and Rar object
                archo, flist = raro_flist(_apath)
            else:
                raise UnpackingError('Unrar is not installed')

        _fpaths = []
        for fpath in fpaths:
            _folder = os.path.abspath(os.path.dirname(fpath))
            _fname = os.path.basename(fpath)
            for f in flist:
                if os.path.basename(f) == _fname:
                    archo.extract(f, _folder)
                    src = os.path.join(_folder, f).replace('/', os.sep)
                    # rename just extracted archive
                    print(src, fpath)
                    move(src, fpath)
                    _fpaths.append(fpath)

        return _fpaths
    else:
        raise UnpackingError('Not supported format')


def unzip_one_file(apath, name, fname=None):
    archo, flist = zipo_flist(apath)
    folder = os.path.abspath(os.path.dirname(apath))
    if len(flist) > 1:
        raise UnpackingError('There are more than one file in Zip archive.')

    _fname = flist[0]

    if fname:
        if basename(fname) in flist:
            _fname = fname

    archo.extract(_fname, folder)
    src = join(folder, _fname)
    ext = os.path.splitext(_fname)[1].strip('.')
    new_apath = build_fpath(folder, name, ext)
    move(src, new_apath)
    return new_apath


def zipo_flist(apath):
    zipo = ZipFile(apath)
    return zipo, zipo.namelist()


def zip_flist(apath):
    zipo = ZipFile(apath)
    return zipo.namelist()


def raro_flist(apath):
    raro = RarFile(apath)
    return raro, raro.namelist()
