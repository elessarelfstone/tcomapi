import sys
from collections import deque

from box import Box
from tqdm import tqdm

from tcomapi.common.constants import SERVER_IS_DOWN, PROLOGUE
from tcomapi.common.data_file_helper import DataFileHelper
from tcomapi.common.utils import is_server_up
from tcomapi.sgov.api import SgovJuridicalsParser
from tcomapi.sgov.cli import parse_args


class TComApiTqdm(tqdm):
    def update_to(self, n, s):
        self.update(n)
        self.set_description(s)


def main():
    args = parse_args()
    p = Box(vars(args))
    if not is_server_up(SgovJuridicalsParser.host):
        print(SERVER_IS_DOWN)
        sys.exit()

    fm = DataFileHelper(p.fpath, limit_fsize=p.fsize)

    print(PROLOGUE)
    ids = deque(fm.load_ids())
    with TComApiTqdm(total=fm.all_count) as pbar:
        pbar.update(fm.prs_count)
        pr = SgovJuridicalsParser(p.fpath, semaphore_limit=p.semlimit,
                                  ratelimit=p.ratelimit)
        try:
            pr.process(ids, fm.curr_file, fm.parsed_file, hook=pbar.update_to)
        except KeyboardInterrupt:
            sys.exit(1)

        except Exception as e:
            raise


if __name__ == "__main__":
    main()
