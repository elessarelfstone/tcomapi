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


config_path = os.path.join(CONFIG_DIR, 'jwaddress.conf')
add_config_path(config_path)


class kgd_jwaddress(BaseConfig):
    url = luigi.Parameter(default='')
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class JwaddressParse(ParseWebExcelFile):
    def run(self):
        rows = parse(self.input().path, Row, skiprows=self.skiptop)
        save_csvrows(self.output().path, [attr.astuple(r) for r in rows])


@requires(JwaddressParse)
class GzipJwaddressToFtp(GzipToFtp):
    pass


class Jwaddress(luigi.WrapperTask):
    def requires(self):
        return GzipJwaddressToFtp(url=kgd_jwaddress().url, name=kgd_jwaddress().name(),
                                  skiptop=kgd_jwaddress().skiptop)


if __name__ == '__main__':
    luigi.run()
