""" Streets codifier """
import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from settings import CONFIG_DIR, TMP_DIR
from tasks.base import GzipToFtp, BaseConfig, WebExcelFileParsingToCsv


@attr.s
class Row:
    code = attr.ib(default='')
    namekz = attr.ib(default='')
    nameru = attr.ib(default='')

    def __attrs_post_init__(self):
        self.namekz, self.nameru = self.nameru, self.namekz


url = 'http://old.stat.gov.kz/getImg?id=WC16200004875'


@requires(WebExcelFileParsingToCsv)
class GzipKurkToFtp(GzipToFtp):
    pass


class Kurk(luigi.WrapperTask):
    def requires(self):
        return GzipKurkToFtp(url=url,
                             name='sgov_kurk',
                             struct=Row,
                             directory=TMP_DIR,
                             skiptop=3)


if __name__ == '__main__':
    luigi.run()
