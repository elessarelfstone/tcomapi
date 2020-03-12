import json
import os
import re
from datetime import date

import luigi
import attr
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tasks.base import ParseJavaScript, GzipToFtp, BaseConfig
from tcomapi.common.javascript import parse_json_from_js
from tcomapi.common.utils import save_to_csv
from settings import CONFIG_DIR
# from tcomextdata.tasks.base import GzipToFtp, BaseConfig


@attr.s
class Row:
    id = attr.ib(converter=lambda x: str(x))
    date = attr.ib(default='')
    rate = attr.ib(default='')


config_path = os.path.join(CONFIG_DIR, 'refinance.conf')
add_config_path(config_path)


class kgd_refinance(BaseConfig):
    url = luigi.Parameter(default='')
    pattern = luigi.Parameter(default='')


class RefinanceParse(ParseJavaScript):

    pattern = luigi.Parameter(default='')

    def run(self):
        d = parse_json_from_js(self.url, self.pattern)
        rows = [Row(**_d) for _d in d]
        save_to_csv(self.output().path, [attr.astuple(_d) for _d in rows])


@requires(RefinanceParse)
class GzipRefinanceToFtp(GzipToFtp):
    pass


class Refinance(luigi.WrapperTask):
    def requires(self):
        return GzipRefinanceToFtp(url=kgd_refinance().url, name=kgd_refinance().name(),
                                  pattern=kgd_refinance().pattern)


if __name__ == '__main__':
    luigi.run()
