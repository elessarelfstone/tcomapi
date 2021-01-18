import os

import attr
import luigi
from luigi.util import requires

from settings import TMP_DIR
from tasks.base import GzipToFtp, WebExcelFileParsingToCsv


@attr.s
class Row:
    code = attr.ib(default='')
    namekz = attr.ib(default='')
    nameru = attr.ib(default='')

url = 'https://stat.gov.kz/api/getFile/?docId=ESTAT116569'


@requires(WebExcelFileParsingToCsv)
class GzipKpvedToFtp(GzipToFtp):
    pass


class Kpved(luigi.WrapperTask):
    def requires(self):
        return GzipKpvedToFtp(url=url,
                              name='sgov_kpved',
                              monthly=True,
                              struct=Row,
                              directory=TMP_DIR,
                              skiptop=3)


if __name__ == '__main__':
    luigi.run()
