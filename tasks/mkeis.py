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


url = 'https://stat.gov.kz/api/getFile/?docId=ESTAT316776'


@requires(WebExcelFileParsingToCsv)
class GzipMkeisToFtp(GzipToFtp):
    pass


class Mkeis(luigi.WrapperTask):
    def requires(self):
        return GzipMkeisToFtp(url=url,
                              monthly=True,
                              name='sgov_mkeis',
                              struct=Row,
                              directory=TMP_DIR,
                              skiptop=4)


if __name__ == '__main__':
    luigi.run()
