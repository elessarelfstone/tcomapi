import luigi
import attr
from luigi.util import requires

from settings import TMP_DIR
from tasks.base import GzipToFtp, BaseConfig, WebExcelFileParsingToCsv


@attr.s
class Row:
    code = attr.ib(default='')
    namekz = attr.ib(default='')
    nameru = attr.ib(default='')
    lv0 = attr.ib(default='')
    lv1 = attr.ib(default='')
    lv2 = attr.ib(default='')
    lv3 = attr.ib(default='')


url = 'https://stat.gov.kz/api/getFile/?docId=ESTAT310324'


def update_rows(rows):
    """ Complete each row with levels """
    curr_root = rows[0].code

    for i, r in enumerate(rows):
        if not r.code:
            rows.pop(i)
            continue
        # build new code
        # A, B, C, etc are like roots for a certain code
        if ('.' in r.code) or (r.code.replace('.', '').isdigit()):
            code = f'{curr_root}.{r.code}'
        else:
            code = r.code
            curr_root = r.code

        r.code = r.code.replace('.', '')

        b = code.split('.')
        size = len(b)
        if size == 2:
            r.lv0 = b[0]

        elif size == 3:
            if len(b[2]) == 1:
                r.lv0, r.lv1 = b[0], b[1]
            else:
                r.lv0, r.lv1, r.lv2 = b[0], b[1], f'{b[1]}{b[2][0]}'

        elif size == 4:
            r.lv0, r.lv1, r.lv2, r.lv3 = b[0], b[1], f'{b[1]}{b[2][0]}', f'{b[1]}{b[2]}'


@requires(WebExcelFileParsingToCsv)
class GzipOkedToFtp(GzipToFtp):
    pass


class Oked(luigi.WrapperTask):
    def requires(self):
        return GzipOkedToFtp(url=url,
                             monthly=True,
                             name='sgov_oked',
                             struct=Row,
                             directory=TMP_DIR,
                             skiptop=3,
                             transform_callback=update_rows)


if __name__ == '__main__':
    luigi.run()
