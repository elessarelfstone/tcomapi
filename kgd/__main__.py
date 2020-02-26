import sys
from collections import deque
from os.path import basename

from box import Box
from tqdm import tqdm

import kgd.constants as cnst

from kgd.cli import parse_args
from kgd.common import ParseFilesManager
from kgd.constants import SERVER_IS_DOWN, PROLOGUE, STATUS_EXPLANATION
from kgd.api import KgdTaxPaymentParser
from kgd.utils import is_server_up


def main():
    args = parse_args()
    p = Box(vars(args))

    if not is_server_up(KgdTaxPaymentParser.host):
        print(SERVER_IS_DOWN)
        sys.exit(-1)

    fm = ParseFilesManager(p.fpath)
    pr = KgdTaxPaymentParser(p.token, p.timeout)

    print(PROLOGUE)
    bins = deque(fm.load_ids())

    # print_status_map()
    print(STATUS_EXPLANATION)
    with tqdm(total=fm.all_count) as pbar:
        pbar.update(fm.prs_count)

        while bins:
            incr = 1
            r = False
            if pr.failed:
                _bin = pr.failed.popleft()
                # inform tqdm about reprocessing
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
                    exit()

            except KeyboardInterrupt:
                sys.exit(1)

            except Exception as e:
                print(e)
                sys.exit(-1)

            pbar.update(incr)


if __name__ == "__main__":
    main()
