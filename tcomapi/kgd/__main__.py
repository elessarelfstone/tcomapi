import sys
from collections import deque
from os.path import basename

from box import Box
from tqdm import tqdm

from tcomapi.common.constants import SERVER_IS_DOWN, PROLOGUE, KGD_STATUS_EXPLANATION
from tcomapi.common.utils import is_server_up, fname_noext
from tcomapi.kgd.api import KgdTaxPaymentParser, KgdServerNotAvailableError
from tcomapi.kgd.cli import parse_args


def main():
    args = parse_args()
    p = Box(vars(args))

    if not is_server_up(KgdTaxPaymentParser.host):
        print(SERVER_IS_DOWN)
        sys.exit(-1)

    parser = KgdTaxPaymentParser(fname_noext(p.fpath), p.fpath, p.date_range,
                                 p.token, p.timeout)

    print(PROLOGUE)

    print(KGD_STATUS_EXPLANATION)
    with tqdm(total=parser.source_bids_count) as pbar:
        pbar.update(parser.parsed_bids_count)

        while parser.bids:
            incr = 1
            r = False

            if parser.failed_bids:
                incr = 0
                bid = parser.failed_bids.popleft()
                r = True
            else:
                bid = parser.bids.popleft()
                r = False

            # refresh tqdm
            status = parser.status(bid, r)
            pbar.set_description(status)
            try:
                r = parser.process_bin(bid)
                pbar.update(incr)

            except KgdServerNotAvailableError:
                print(SERVER_IS_DOWN)
                exit()

            except KeyboardInterrupt:
                sys.exit(1)

            except Exception as e:
                print(e)
                sys.exit(-1)


if __name__ == "__main__":
    main()
