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
    indicator = attr.ib(default='', validator=is_float, converter=float_corrector)
    oblrus = attr.ib(default='')
    edizmrus = attr.ib(default='')
    year = attr.ib(default='')
    оblkaz = attr.ib(default='')
    edizmkaz = attr.ib(default='')


class dgov_foodbasket(BaseConfig):
    rep_name = luigi.Parameter(default='')
    url_total = luigi.Parameter(default='')
    versions = luigi.TupleParameter(default=tuple())


config_path = os.path.join(CONFIG_DIR, 'foodbasket.conf')
add_config_path(config_path)


class ParseFoodBasket(ParseElasticApi):

    def run(self):
        rep_url = report_url(self.rep_name)
        versions = self.versions
        if not versions:
            versions = load_versions(rep_url)
        for vs in versions:
            url = data_url(self.rep_name, DGOV_API_KEY, version=vs)
            data = load_data_as_tuple(url, Row)
            save_to_csv(self.output().path, data)


@requires(ParseFoodBasket)
class GzipFoodBasketToFtp(GzipToFtp):
    pass


class FoodBasket(luigi.WrapperTask):

    def requires(self):
        return GzipFoodBasketToFtp(name=dgov_foodbasket().name(),
                                   versions=dgov_foodbasket().versions,
                                   rep_name=dgov_foodbasket().rep_name)


if __name__ == '__main__':
    luigi.run()
