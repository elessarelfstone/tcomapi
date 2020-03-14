import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.correctors import basic_corrector
from tcomapi.common.excel import parse
from tcomapi.common.utils import save_to_csv
from settings import CONFIG_DIR
from tasks.base import GzipToFtp, BaseConfig, ParseWebExcelFile


@attr.s
class Row:
    num = attr.ib(default='')
    bin = attr.ib(default='')
    rnn = attr.ib(default='')
    taxpayer_organization = attr.ib(converter=basic_corrector, default='')
    taxpayer_name = attr.ib(default='')
    owner_name = attr.ib(default='')
    owner_iin = attr.ib(default='')
    owner_rnn = attr.ib(default='')
    court_decision = attr.ib(default='')
    illegal_activity_start_date = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'pseudocompany.conf')
add_config_path(config_path)


class kgd_pseudocompany(BaseConfig):
    url = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class PseudocompanyParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [attr.astuple(r) for r in rows])


@requires(PseudocompanyParse)
class GzipPseudocompanyToFtp(GzipToFtp):
    pass


class Pseudocompany(luigi.WrapperTask):
    def requires(self):
        return GzipPseudocompanyToFtp(url=kgd_pseudocompany().url, name=kgd_pseudocompany().name(),
                                      skiptop=kgd_pseudocompany().skiptop)


if __name__ == '__main__':
    luigi.run()
