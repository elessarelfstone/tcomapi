import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tasks.base import GzipToFtp, BaseConfig, ParseElasticApi
from tcomapi.common.utils import save_to_csv, append_file
from tcomapi.common.correctors import float_corrector
from tcomapi.common.data_verification import is_float
from tcomapi.dgov.api import (load_versions, load_data_as_tuple,
                              data_url, report_url)

from settings import CONFIG_DIR, DGOV_API_KEY

#
# def is_float(value):
#     try:
#         _ = float(value)
#         return True
#     except ValueError:
#         return False


@attr.s
class Row:
    regionkz = attr.ib(default='')
    indicator = attr.ib(default='')
    year = attr.ib(default='')
    edizmru = attr.ib(default='')
    edizmkz = attr.ib(default='')
    regionru = attr.ib(default='')


class dgov_unemplrate(BaseConfig):
    rep_name = luigi.Parameter(default='')
    url_total = luigi.Parameter(default='')
    versions = luigi.TupleParameter(default=tuple())


config_path = os.path.join(CONFIG_DIR, 'unemplrate.conf')
add_config_path(config_path)


class ParseUnemplRate(ParseElasticApi):

    def run(self):
        rep_url = report_url(self.rep_name)
        versions = self.versions
        if not versions:
            versions = load_versions(rep_url)
        for vs in versions:
            url = data_url(self.rep_name, DGOV_API_KEY, version=vs)
            data = load_data_as_tuple(url, Row)
            save_to_csv(self.output().path, data)


@requires(ParseUnemplRate)
class GzipFoodBasketToFtp(GzipToFtp):
    pass


class UnemplRate(luigi.WrapperTask):

    def requires(self):
        return GzipFoodBasketToFtp(name=dgov_unemplrate().name(),
                                   versions=dgov_unemplrate().versions,
                                   rep_name=dgov_unemplrate().rep_name)


if __name__ == '__main__':
    luigi.run()
