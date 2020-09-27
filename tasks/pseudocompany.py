import os

import attr
import luigi
from luigi.configuration.core import add_config_path
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
    illegal_activity_start_date = attr.ib(default='')


url = 'http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/PSEUDO_COMPANY/KZ_ALL/fileName/list_PSEUDO_COMPANY_KZ_ALL.xlsx'


@requires(WebExcelFileParsingToCsv)
class GzipPseudocompanyToFtp(GzipToFtp):
    pass


class Pseudocompany(luigi.WrapperTask):
    def requires(self):
        return GzipPseudocompanyToFtp(url=url,
                                      name='kgd_pseudocompany',
                                      struct=Row,
                                      directory=TMP_DIR,
                                      skiptop=3)


if __name__ == '__main__':
    luigi.run()
