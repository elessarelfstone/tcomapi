import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_csvrows
from settings import CONFIG_DIR, TMP_DIR
from tasks.base import GzipToFtp, BaseConfig, WebExcelFileCustomParsingToCsv, WebExcelFileParsingToCsv


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
    court_decision_no = attr.ib(default='')
    court_decision_date = attr.ib(default='')


url = 'http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/INVALID_REGISTRATION/KZ_ALL/fileName/list_INVALID_REGISTRATION_KZ_ALL.xlsx'


class InvregistrationParse(WebExcelFileCustomParsingToCsv):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_csvrows(self.output().path, [attr.astuple(r) for r in rows])


@requires(WebExcelFileParsingToCsv)
class GzipInvregistrationToFtp(GzipToFtp):
    pass


class Invregistration(luigi.WrapperTask):
    def requires(self):
        return GzipInvregistrationToFtp(url=url,
                                        name='kgd_invregistration',
                                        struct=Row,
                                        directory=TMP_DIR,
                                        skiptop=3
                                        )


if __name__ == '__main__':
    luigi.run()
