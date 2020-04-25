import os

import luigi
import attr
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.correctors import basic_corrector
from tcomapi.common.excel import parse
from tcomapi.common.utils import save_to_csv
from settings import CONFIG_DIR
from tasks.base import GzipToFtp, BaseConfig, ParseWebExcelFile


@attr.s
class Row:
    code = attr.ib(default='')
    namekz = attr.ib(default='', converter=basic_corrector)
    nameru = attr.ib(default='', converter=basic_corrector)
    lv0 = attr.ib(default='')
    lv1 = attr.ib(default='')
    lv2 = attr.ib(default='')
    lv3 = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'oked.conf')
add_config_path(config_path)


class sgov_oked(BaseConfig):
    url = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


def update_rows(rows):
    """ Complete each row with levels """
    curr_root = rows[0].code

    for i, r in enumerate(rows):
        if not r.code:
            rows.pop(i)
            continue
        # build new code
        # A, B, C, etc are like roots for a certain code
        if ('.' in r.code) or (r.code.replace('.', '').isdigit()):
            code = f'{curr_root}.{r.code}'
        else:
            code = r.code
            curr_root = r.code

        r.code = r.code.replace('.', '')

        b = code.split('.')
        size = len(b)
        if size == 2:
            r.lv0 = b[0]

        elif size == 3:
            if len(b[2]) == 1:
                r.lv0, r.lv1 = b[0], b[1]
            else:
                r.lv0, r.lv1, r.lv2 = b[0], b[1], f'{b[1]}{b[2][0]}'

        elif size == 4:
            r.lv0, r.lv1, r.lv2, r.lv3 = b[0], b[1], f'{b[1]}{b[2][0]}', f'{b[1]}{b[2]}'


class OkedParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=sgov_oked().skiptop)
        print(len(rows))
        update_rows(rows)
        save_to_csv(self.output().path, [attr.astuple(r) for r in rows])


@requires(OkedParse)
class GzipOkedToFtp(GzipToFtp):
    pass


class Oked(luigi.WrapperTask):
    def requires(self):
        return GzipOkedToFtp(url=sgov_oked().url, name=sgov_oked().name(),
                             skiptop=sgov_oked().skiptop)


if __name__ == '__main__':
    luigi.run()
