import os

import attr
import luigi
from luigi.util import requires

from settings import TMP_DIR
from tasks.base import GzipToFtp, WebExcelFileParsingToCsv


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


url = 'http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/BANKRUPT/KZ_ALL/fileName/list_BANKRUPT_KZ_ALL.xlsx'


@requires(WebExcelFileParsingToCsv)
class GzipBankruptToFtp(GzipToFtp):
    pass


class Bankrupt(luigi.WrapperTask):
    def requires(self):
        return GzipBankruptToFtp(url=url,
                                 name='kgd_bunkrupt',
                                 directory=TMP_DIR,
                                 struct=Row,
                                 skiptop=3)


if __name__ == '__main__':
    luigi.run()
