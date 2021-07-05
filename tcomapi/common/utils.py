import os
import gzip
import hashlib
import json
from typing import Dict

import requests
import urllib3
import shutil
import socket
import subprocess as subp
from collections import namedtuple, Counter
from os.path import basename
from urllib.parse import urlparse

import attr
from requests import ConnectionError, HTTPError
from requests.exceptions import ConnectTimeout

from tcomapi.common.correctors import clean_for_csv
from tcomapi.common.constants import CSV_SEP
from tcomapi.common.exceptions import ExternalSourceError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# handy working with formats
Formats = namedtuple('Formats', ['extension', 'mime', 'offset', 'signature'])

REQUEST_TIMEOUT = 10
FILE_SUFF_DATE_FORMAT = '%Y%m%d'


def date_for_fname(dt, date_format=FILE_SUFF_DATE_FORMAT, for_month=False):
    if for_month:
        _dt = dt.replace(day=1)
    else:
        _dt = dt
    return _dt.strftime(date_format)


def read_lines(fpath):
    """ Return rows of file as list """
    with open(fpath, "r", encoding="utf-8") as f:
        lines = [b.rstrip() for b in f.readlines()]

    return lines


def read_file(fpath):
    """ Return all rows of file as string"""
    with open(fpath, 'r', encoding="utf8") as f:
        data = f.read()

    return data


def append_file(fpath, data):
    """ Add new line to file"""
    with open(fpath, 'a+', encoding="utf8") as f:
        f.write(data + '\n')


def fname_noext(fpath):
    return os.path.splitext(basename(fpath))[0]


def fpath_noext(fpath):
    return os.path.splitext(fpath)[0]


def fpath_noext2(fpath):
    directory = os.path.dirname(fpath)
    bsname = os.path.splitext(basename(fpath))[0]
    return os.path.join(directory, bsname)


def parsed_fpath(fpath, ext='prs'):
    return '.'.join((fpath_noext(fpath), ext))


def success_fpath(fpath, ext='success'):
    return '.'.join((fpath_noext(fpath), ext))


def run_command(args, encoding="utf-8", **kwargs):
    p = subp.Popen(args, stdout=subp.PIPE, stderr=subp.PIPE, **kwargs)
    result, err = p.communicate()
    if p.returncode > 1:
        raise IOError(err)

    if p.returncode == 1 and result:
        return result.decode(encoding).strip().split()[0]

    return None


def is_server_up(address, port=443):
    r = True
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        result = sock.connect_ex((address, int(port)))
    except Exception:
        r = False

    return r


def dict_to_csvrow(raw_dict, struct):
    """ Convert given dict into tuple using
    given structure(attr class)."""

    # cast each keys's name of dict to lower case
    __raw_dict = {k.lower(): v for k, v in raw_dict.items() if k.lower()}

    # get fields of structure
    keys = [a.name for a in attr.fields(struct)]

    _dict = {}
    # build new dict with fields specified in struct
    for k in keys:
        if k in __raw_dict.keys():
            _dict[k] = __raw_dict[k]

    # _dict = {k: _dict[k] for k in keys}

    # wrap in struct
    attr_obj = struct(**_dict)

    return attr.astuple(attr_obj)


def save_csvrows(fpath, recs, sep=None, quoter=None):
    """ Save list of tuples as csv rows to file """

    if sep:
        _sep = sep
    else:
        _sep = CSV_SEP

    if quoter is None:
        _q = ''
    else:
        _q = quoter

    with open(fpath, 'a+', encoding="utf-8") as f:
        for rec in recs:
            # clean
            _rec = [clean_for_csv(v) for v in rec]
            # quoting
            _rec = [f'{_q}{v}{_q}' for v in _rec]
            row = _sep.join(_rec)
            f.write(row + '\n')

    return os.path.getsize(fpath)


def gziped_fname(fpath, suff=None):
    ext = os.path.basename(fpath).split('.')[1]

    if suff:
        result = '{}_{}.{}.gzip'.format(fname_noext(fpath), suff, ext)
    else:
        result = '{}.{}.gzip'.format(fname_noext(fpath), ext)

    return result


def gzip_file(fpath):
    """ Gzip given file"""
    _dir = os.path.dirname(os.path.abspath(fpath))
    _fpath = os.path.join(_dir, gziped_fname(fpath))

    # _fpath = gziped_fpath(fpath)
    with open(fpath, 'rb') as f_in:
        with gzip.open(_fpath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return _fpath


def get(url: str, headers=None, timeout=None) -> str:

    # we always specify verify to False
    # cause we don't use certificate into
    # Kazakhtelecom network
    r = requests.get(url, verify=False, headers=headers, timeout=timeout)
    # try:
    if r.status_code != 200:
        r.raise_for_status()
    # except :
    #     raise ExternalSourceError()
    return r.text


def post(url: str, request: str,  headers=None, timeout=None) -> str:

    # we always specify verify to False
    # cause we don't use certificate into
    # Kazakhtelecom network
    r = requests.post(url, request, verify=False, headers=headers, timeout=timeout)
    # try:
    if r.status_code != 200:
        r.raise_for_status()
    # except :
    #     raise ExternalSourceError()
    return r.text


def load_content(url: str, headers=None, timeout=None) -> str:

    r = None

    try:
        r = get(url, headers=headers, timeout=timeout)
    except (ConnectionError, ConnectTimeout) as e:
        host = urlparse(url).netloc
        raise ExternalSourceError('Could not connect to ' + host)

    except Exception:
        raise

    return r


def load_url_content(url, headers=None, timeout=None):
    """ Just simply url data"""
    try:
        # we always specify verify to False
        # cause we don't use certificate into
        # Kazakhtelecom network
        with requests.get(url, verify=False, timeout=timeout,
                          headers=headers) as r:
            r.raise_for_status()

        return r.text
    except Exception as e:
        raise ExternalSourceError('Could not load html.' + url)


def formats():
    """ Load file formats we work with. For checking and proper saving."""
    # load formats from file
    data = read_file(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "file_formats.json")
    )

    # wrap in Formats struct
    _formats = []
    for f in json.loads(data):
        _formats.append(Formats(**f))

    return _formats


def save_webfile(url, fpath):

    size = download(url, fpath)
    if size == 0:

        raise ExternalSourceError("Empty file")

    _format = identify_format(fpath)

    if not _format:
        raise ExternalSourceError("File's signature does not match its type.")

    return _format


def identify_webfileformat(url, verify=False):
    """ Return file format for remote web file."""
    _formats = formats()

    # request to get mime type of file
    s = requests.Session()
    r = s.head(url, verify=verify)
    mime = r.headers['content-type']
    for frmt in _formats:
        if frmt.mime == mime:
            return frmt.extension, frmt.signature, frmt.offset

    return None


def identify_format(fpath):
    """ Read signature of file and return format if it's supported """

    _formats = formats()

    # read first N bytes
    with open(fpath, "rb") as file:
        # 500 bytes are enough
        header = file.read(300)

    # convert to hex
    stream = " ".join(['{:02X}'.format(byte) for byte in header])

    for frmt in _formats:
        # if there is offset
        offset = frmt.offset * 2 + frmt.offset
        # print(frmt.signature)
        # print(stream[offset:len(frmt.signature) + offset])
        if frmt.signature == stream[offset:len(frmt.signature) + offset]:
            return frmt.extension

    return None


def build_fname(fname, ext, suff=None):
    return f'{fname}_{suff}.{ext}' if suff else f'{fname}.{ext}'


def build_fpath(fdir, fname, ext):
    """ Return concatenated file's path """
    fpath = os.path.join(fdir, f'{fname}.{ext}')
    return fpath


def build_webfpath(url, fdir, fname):
    """ Return possible concatenated path for remote web file """
    _format = identify_webfileformat(url)

    if not _format:
        return None

    fpath = build_fpath(fdir, fname, _format[0])
    return fpath


def check_fileheader(fpath):
    """ Check file's signature. If match return True. """
    with open(fpath, "rb") as file:
        # 300 bytes are enough
        header = file.read(300)

    # convert to hex
    stream = " ".join(['{:02X}'.format(byte) for byte in header])

    # check
    for frmt in formats():
        offset = frmt.offset * 2 + frmt.offset
        if frmt.signature == stream[offset:len(frmt.signature) + offset]:
            return frmt.extension

    return None


def get_hash(f_path, mode='sha256'):
    """ Get hash of file"""

    h = hashlib.new(mode)

    with open(f_path, 'rb') as file:
        block = file.read(4096)
        while block:
            h.update(block)
            block = file.read(4096)

    return h.hexdigest()


def download(url, fpath):
    """ Download file using stream """
    try:
        # we always specify verify to False
        # cause we don't use certificate into
        # Kazakhtelecom network
        with requests.get(url, stream=True, verify=False) as r:
            r.raise_for_status()
            f_size = 0
            with open(fpath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        f_size += len(chunk)

        return f_size

    except (ConnectionError, HTTPError) as e:
        if os.path.exists(fpath):
            os.remove(fpath)
        raise ExternalSourceError('Could not download file {}'.format(fpath))


def get_stata(c: Counter):
    return ' '.join(('{}:{}'.format(k, v) for k, v in c.items()))


def apply_filter_to_dict(d: Dict, column_filter: Dict) -> Dict:
    """
        Return dict with removed excess columns
        and renamed target columns
    """
    for k in column_filter.keys():
        if k not in d.keys():
            del d[k]

    _d = {}

    for old_key, new_key in column_filter.items():
        _d[new_key] = d.pop(old_key)

    return _d


def get_lastrow_ncolumn_value_in_csv(fpath, column_num: int, sep=';'):
    if os.path.exists(fpath) and (os.stat(fpath).st_size != 0):
        r = subp.check_output("awk -F'{}' 'END{{print $1}}' {}".format(sep, fpath))
        return r.split()[column_num].decode(encoding='utf-8')

    return None


def get_file_lines_count(fpath: int):
    if os.path.exists(fpath):
        r = subp.check_output("wc -l {}".format(fpath))
        return int(r.decode(encoding='utf-8').split()[0]) if r else 0

    return None
