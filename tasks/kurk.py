""" Streets codifier """
import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_csvrows, swap_elements as swap
from settings import CONFIG_DIR, TMP_DIR
from tasks.base import GzipToFtp, BaseConfig, ParseWebExcelFile, WebExcelFileParsingToCsv


@attr.s
class Row:
    code = attr.ib(default='')
    namekz = attr.ib(default='')
    nameru = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'kurk.conf')
add_config_path(config_path)


class sgov_kurk(BaseConfig):
    url = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class KurkParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_csvrows(self.output().path, [swap(attr.astuple(r), 1, 2) for r in rows])


class KurkParse2(WebExcelFileParsingToCsv):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_csvrows(self.output().path, [swap(attr.astuple(r), 1, 2) for r in rows])


@requires(KurkParse2)
class GzipKurkToFtp2(GzipToFtp):
    pass


class Kurk2(luigi.WrapperTask):
    def requires(self):
        return GzipKurkToFtp2(directory=TMP_DIR,
                              url=sgov_kurk().url,
                              name=sgov_kurk().name(),
                              skiptop=sgov_kurk().skiptop)


@requires(KurkParse)
class GzipKurkToFtp(GzipToFtp):
    pass


class Kurk(luigi.WrapperTask):
    def requires(self):
        return GzipKurkToFtp(url=sgov_kurk().url,
                             name=sgov_kurk().name(),
                             skiptop=sgov_kurk().skiptop)


if __name__ == '__main__':
    luigi.run()
