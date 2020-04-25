import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_to_csv
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
    court_decision = attr.ib(default='')
    court_decision_date = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'bankrupt.conf')
add_config_path(config_path)


class kgd_bankrupt(BaseConfig):
    url = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class BankruptParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [attr.astuple(r) for r in rows])


@requires(BankruptParse)
class GzipBankruptToFtp(GzipToFtp):
    pass


class Bankrupt(luigi.WrapperTask):
    def requires(self):
        return GzipBankruptToFtp(url=kgd_bankrupt().url,
                                 name=kgd_bankrupt().name(),
                                 skiptop=kgd_bankrupt().skiptop)


if __name__ == '__main__':
    luigi.run()
