import os
import socket
import subprocess as subp

import attr


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


def prepare(row, struct):
    """ Convert dict into tuple using
    given structure(attr class, dataclass)."""

    # cast all fields name of struct in lowercase
    _p_dict = {k.lower(): v for k, v in row.items() if k.lower()}

    # wrap in struct
    data = struct(**_p_dict)

    return attr.astuple(data)
