import os

import luigi
import attr
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tasks.base import ParseJavaScript, GzipToFtp, BaseConfig
from tcomapi.common.javascript import parse_json_from_js
from tcomapi.common.utils import save_to_csv
from settings import CONFIG_DIR


@attr.s
class Row:
    id = attr.ib(converter=lambda x: str(x))
    value = attr.ib(converter=lambda x: str(x))
    date = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'mrp.conf')
add_config_path(config_path)


class kgd_mrp(BaseConfig):
    url = luigi.Parameter(default='')
    pattern = luigi.Parameter(default='')


class MrpParse(ParseJavaScript):

    pattern = luigi.Parameter(default='')

    def run(self):
        d = parse_json_from_js(self.url, self.pattern)
        # wrap each row and get tuple
        rows = [attr.astuple(Row(**_d)) for _d in d]
        save_to_csv(self.output().path, rows)


@requires(MrpParse)
class GzipMrpToFtp(GzipToFtp):
    pass


class Mrp(luigi.WrapperTask):
    def requires(self):
        return GzipMrpToFtp(url=kgd_mrp().url, name=kgd_mrp().name(),
                            pattern=kgd_mrp().pattern)


if __name__ == '__main__':
    luigi.run()
