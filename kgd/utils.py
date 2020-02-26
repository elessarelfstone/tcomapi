import os
import socket
import subprocess as subp
from urllib3 import Retry

import requests
from requests.adapters import HTTPAdapter


def load_lines(fpath):
    """ Return rows of file as list"""

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


def get_base_fpath(fpath):
    _dir = os.path.dirname(fpath)
    _basename = os.path.splitext(os.path.basename(fpath))[0]
    return os.path.join(_dir, _basename)


def requests_retry_session(retries, backoff,
                           status_forcelist, session=None):
    """ Make request with timeout and retries """
    session = session or requests.Session()
    retry = Retry(total=retries,read=retries, connect=retries,
                  backoff_factor=backoff, status_forcelist=status_forcelist,
                  method_whitelist=frozenset(['GET', 'POST'])
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session


def run_command(args, encoding="utf-8", **kwargs):
    p = subp.Popen(args, stdout=subp.PIPE, stderr=subp.PIPE, **kwargs)
    result, err = p.communicate()
    if p.returncode > 1:
        raise IOError(err)

    if p.returncode == 1 and result:
        return result.decode(encoding).strip().split()[0]

    return None


def is_server_up(address, port=443):
    is_up = True
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        result = sock.connect_ex((address, int(port)))
    except Exception:
        is_up = False

    return is_up
