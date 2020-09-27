import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tasks.base import GzipToFtp, WebExcelFileParsingToCsv
from settings import TMP_DIR


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
    owner_no = attr.ib(default='')
    order_date = attr.ib(default='')


url = 'http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/INACTIVE/KZ_ALL/fileName/list_INACTIVE_KZ_ALL.xlsx'


@requires(WebExcelFileParsingToCsv)
class GzipInactiveToFtp(GzipToFtp):
    pass


class Inactive(luigi.WrapperTask):
    def requires(self):
        return GzipInactiveToFtp(url=url,
                                 name='kgd_inactive',
                                 directory=TMP_DIR,
                                 struct=Row,
                                 skiptop=3)


if __name__ == '__main__':
    luigi.run()
