import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_csvrows
from settings import CONFIG_DIR
from tasks.base import GzipToFtp, BaseConfig, ParseWebExcelFile


@attr.s
class Row:
    num = attr.ib(default='')
    bin = attr.ib(default='')
    rnn = attr.ib(default='')
    taxpayer_organization = attr.ib(default='')
    taxpayer_name = attr.ib(default='')
    owner_name = attr.ib(default='')
    owner_iin = attr.ib(default='')
    owner_rnn = attr.ib(default='')
    inspection_act_no = attr.ib(default='')
    inspection_date = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'taxviolators.conf')
add_config_path(config_path)


class kgd_taxviolators(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class TaxViolatorsParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_csvrows(self.output().path, [attr.astuple(r) for r in rows], ';')


@requires(TaxViolatorsParse)
class GzipTaxViolatorsToFtp(GzipToFtp):
    pass


class TaxViolators(luigi.WrapperTask):
    def requires(self):
        return GzipTaxViolatorsToFtp(url=kgd_taxviolators().url, name=kgd_taxviolators().name(),
                                     skiptop=kgd_taxviolators().skiptop)


if __name__ == '__main__':
    luigi.run()
