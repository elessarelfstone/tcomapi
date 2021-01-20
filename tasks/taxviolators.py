import os

import attr
import luigi
from luigi.util import requires

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_csvrows
from settings import TMP_DIR
from tasks.base import GzipToFtp, WebExcelFileCustomParsingToCsv, WebExcelFileParsingToCsv


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


url = 'http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/VIOLATION_TAX_CODE/KZ_ALL/fileName/list_VIOLATION_TAX_CODE_KZ_ALL.xlsx'


@requires(WebExcelFileParsingToCsv)
class GzipTaxViolatorsToFtp(GzipToFtp):
    pass


class TaxViolators(luigi.WrapperTask):
    def requires(self):
        return GzipTaxViolatorsToFtp(url=url,
                                     monthly=True,
                                     name='kgd_taxviolators',
                                     struct=Row,
                                     directory=TMP_DIR,
                                     skiptop=3)


if __name__ == '__main__':
    luigi.run()
