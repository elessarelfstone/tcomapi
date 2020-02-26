import sys
from collections import deque
from os.path import basename

from box import Box
from tqdm import tqdm

import kgd.constants as cnst

from kgd.cli import parse_args
from kgd.common import ParseFilesManager
from kgd.constants import SERVER_IS_DOWN
from kgd.api import KgdTaxPaymentParser
from kgd.utils import is_server_up


def print_loading():
    # TODO One print function
    print('There could be parsed BINs.')
    print('It may take some time. Building list ...')


def print_status_map():
    # TODO One print function and read text from file
    print('> rqe - count of BINs we got KGD API error with. Those BINs are not supposed to be reprocessed.')
    print('> rse - count of occurrences when we got bad response(KGD requests count limitation, ')
    print('html(from proxy e.g. squid) or some other trash not xml formatted). Those BINs are supposed to be reprocessed.')
    print('> se - count of occurrences when we failed(connection, network, 500, etc ). ')
    print(' Those BINs are supposed to be reprocessed. ')
    print('> s - count of BINs we successfully processed')
    print('> R - indicates reprocessing')
    print('>> All these indicators are actual for only this launch!')


def main():
    args = parse_args()
    p = Box(vars(args))

    if not is_server_up(KgdTaxPaymentParser.host):
        print(SERVER_IS_DOWN)
        sys.exit()

    fm = ParseFilesManager(p.fpath)
    pr = KgdTaxPaymentParser(p.token, p.timeout)

    print_loading()
    bins = deque(fm.load_ids())

    print_status_map()
    with tqdm(total=fm.all_count) as pbar:
        pbar.update(fm.prs_count)

        while bins:

            incr = 1
            r = False

            if pr.failed:
                # inform tqdm about reprocessing
                _bin = pr.failed.popleft()
                incr = 0
                r = True
            else:
                _bin = bins.popleft()

            # refresh tqdm
            fname = basename(fm.curr_file)
            status = pr.status(pr.stat, _bin, fname, r)
            pbar.set_description(status)
            try:
                r = pr.process_id(_bin, p.date_range, fm.curr_file, fm.parsed_file)
                if r < 0:
                    print(SERVER_IS_DOWN)
                    exit(-1)

            except KeyboardInterrupt:
                sys.exit(1)

            except Exception as e:
                print(e)
                sys.exit(-1)

            pbar.update(incr)


if __name__ == "__main__":
    main()
