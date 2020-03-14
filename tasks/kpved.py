import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_to_csv
from settings import CONFIG_DIR
from tasks.base import GzipToFtp, BaseConfig, ParseWebExcelFile


@attr.s
class Row:
    code = attr.ib(default='')
    namekz = attr.ib(default='')
    nameru = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'kpved.conf')
add_config_path(config_path)


class sgov_kpved(BaseConfig):
    url = luigi.Parameter(default='')
    # name = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class KpvedParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_to_csv(self.output().path, [attr.astuple(r) for r in rows])


@requires(KpvedParse)
class GzipKpvedToFtp(GzipToFtp):
    pass


class Kpved(luigi.WrapperTask):
    def requires(self):
        return GzipKpvedToFtp(url=sgov_kpved().url, name=sgov_kpved().name(),
                              skiptop=sgov_kpved().skiptop)


if __name__ == '__main__':
    luigi.run()
