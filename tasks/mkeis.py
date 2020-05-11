import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_csvrows
from settings import CONFIG_DIR
from tasks.base import GzipToFtp, BaseConfig, ParseWebExcelFile


@attr.s
class Row:
    code = attr.ib(default='')
    namekz = attr.ib(default='')
    nameru = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'mkeis.conf')
add_config_path(config_path)


class sgov_mkeis(BaseConfig):
    url = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class MkeisParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_csvrows(self.output().path, [attr.astuple(r) for r in rows])


@requires(MkeisParse)
class GzipMkeisToFtp(GzipToFtp):
    pass


class Mkeis(luigi.WrapperTask):
    def requires(self):
        return GzipMkeisToFtp(url=sgov_mkeis().url, name=sgov_mkeis().name(),
                              skiptop=sgov_mkeis().skiptop)


if __name__ == '__main__':
    luigi.run()
