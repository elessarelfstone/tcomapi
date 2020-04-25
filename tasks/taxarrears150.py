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
    region = attr.ib(default='')
    office_of_tax_enforcement = attr.ib(default='')
    ote_id = attr.ib(default='')
    bin = attr.ib(default='')
    rnn = attr.ib(default='')
    taxpayer_organization_ru = attr.ib(default='')
    taxpayer_organization_kz = attr.ib(default='')
    last_name_kz = attr.ib(default='')
    first_name_kz = attr.ib(default='')
    middle_name_kz = attr.ib(default='')
    last_name_ru = attr.ib(default='')
    first_name_ru = attr.ib(default='')
    middle_name_ru = attr.ib(default='')
    owner_iin = attr.ib(default='')
    owner_rnn = attr.ib(default='')
    owner_name_kz = attr.ib(default='')
    owner_name_ru = attr.ib(default='')
    economic_sector = attr.ib(default='')
    total_due = attr.ib(default='')
    sub_total_main = attr.ib(default='')
    sub_total_late_fee = attr.ib(default='')
    sub_total_fine = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'taxarrears150.conf')
add_config_path(config_path)


class kgd_taxarrears150(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class TaxArrears150Parse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [attr.astuple(r) for r in rows])


@requires(TaxArrears150Parse)
class GzipDebtorsToFtp(GzipToFtp):
    pass


class TaxArrears150(luigi.WrapperTask):
    def requires(self):
        return GzipDebtorsToFtp(url=kgd_taxarrears150().url,
                                name=kgd_taxarrears150().name(),
                                skiptop=kgd_taxarrears150().skiptop)


if __name__ == '__main__':
    luigi.run()
