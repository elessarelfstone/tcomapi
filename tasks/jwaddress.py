import os

import attr
import luigi
from luigi.util import requires


from settings import CONFIG_DIR, TMP_DIR
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
    inspection_act_no = attr.ib(default='')
    inspection_date = attr.ib(default='')


url = 'http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/WRONG_ADDRESS/KZ_ALL/fileName/list_WRONG_ADDRESS_KZ_ALL.xlsx'


@requires(WebExcelFileParsingToCsv)
class GzipJwaddressToFtp(GzipToFtp):
    pass


class Jwaddress(luigi.WrapperTask):
    def requires(self):
        return GzipJwaddressToFtp(url=url,
                                  name='kgd_jwaddress',
                                  monthly=True,
                                  directory=TMP_DIR,
                                  struct=Row,
                                  skiptop=3)


if __name__ == '__main__':
    luigi.run()
