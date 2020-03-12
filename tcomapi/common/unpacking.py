import os
from fnmatch import fnmatch
from shutil import which, move
from zipfile import ZipFile
from rarfile import RarFile


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


def unpack(apath, frmt, fpaths):
    """ Unpack files from archive. Support only rar, zip archives """

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
                    move(src, fpath)
                    _fpaths.append(fpath)

        return _fpaths
    else:
        raise UnpackingError('Not supported format')


def zipo_flist(apath):
    zipo = ZipFile(apath)
    return zipo, zipo.namelist()


def raro_flist(apath):
    raro = RarFile(apath)
    return raro, raro.namelist()
