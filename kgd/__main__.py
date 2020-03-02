import sys
from collections import deque
from os.path import basename

from box import Box
from tqdm import tqdm

from kgd.cli import parse_args
from common.data_file_helper import DataFileHelper
from common.constants import SERVER_IS_DOWN, PROLOGUE, KGD_STATUS_EXPLANATION
from kgd.api import KgdTaxPaymentParser
from common.utils import is_server_up


def main():
    args = parse_args()
    p = Box(vars(args))

    if not is_server_up(KgdTaxPaymentParser.host):
        print(SERVER_IS_DOWN)
        sys.exit(-1)

    fm = DataFileHelper(p.fpath, limit_fsize=p.fsize)
    pr = KgdTaxPaymentParser(p.token, p.timeout)

    print(PROLOGUE)
    bins = deque(fm.load_ids())

    print(KGD_STATUS_EXPLANATION)
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
                r = pr.process_bin(_bin, p.date_range, fm.curr_file, fm.parsed_file)

                if r < 0:
                    print(SERVER_IS_DOWN)
                    exit()

                pbar.update(incr)
            except KeyboardInterrupt:
                sys.exit(1)

            except Exception as e:
                print(e)
                sys.exit(-1)


if __name__ == "__main__":
    main()
