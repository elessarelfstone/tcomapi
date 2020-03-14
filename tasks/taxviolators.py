import os
from dataclasses import dataclass, astuple

import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomextdata.lib.excel import parse
from tcomextdata.lib.utils import save_to_csv
from settings import CONFIG_DIR
from tcomextdata.tasks.base import GzipToFtp, BaseConfig, ParseWebExcelFile


@dataclass
class Row:
    num: int
    bin: str
    rnn: str
    taxpayer_organization: str
    taxpayer_name: str
    owner_name: str
    owner_iin: str
    owner_rnn: str
    inspection_act_no: str
    inspection_date: str


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
        save_to_csv(self.output().path, [astuple(r) for r in rows], ';')


@requires(TaxViolatorsParse)
class GzipTaxViolatorsToFtp(GzipToFtp):
    pass


class TaxViolators(luigi.WrapperTask):
    def requires(self):
        return GzipTaxViolatorsToFtp(url=kgd_taxviolators().url, name=kgd_taxviolators().name(),
                                     skiptop=kgd_taxviolators().skiptop)


if __name__ == '__main__':
    luigi.run()
