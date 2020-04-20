import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tasks.base import GzipToFtp, BaseConfig, ParseElasticApi
from tcomapi.common.utils import save_to_csv, append_file
from tcomapi.common.correctors import float_corrector
from tcomapi.common.data_verification import is_float
from tcomapi.dgov.api import (load_datasets, load_data_as_tuple,
                              build_query_url, BIG_QUERY_SLICE_SIZE, load_total)

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
    url = luigi.Parameter(default='')
    url_data_template = luigi.Parameter(default='')
    url_total = luigi.Parameter(default='')
    verisons = luigi.TupleParameter(default=tuple())


config_path = os.path.join(CONFIG_DIR, 'foodbasket.conf')
add_config_path(config_path)


class ParseFoodBasket(ParseElasticApi):

    def run(self):
        versions = self.datasets
        if not versions:
            versions = load_datasets(self.url)
        for ds in versions:
            url = build_query_url(self.url_data_template, ds, DGOV_API_KEY)
            data = load_data_as_tuple(url, Row)
            save_to_csv(self.output().path, data)


@requires(ParseFoodBasket)
class GzipFoodBasketToFtp(GzipToFtp):
    pass


class FoodBasket(luigi.WrapperTask):

    def requires(self):
        return GzipFoodBasketToFtp(url=dgov_foodbasket().url, name=dgov_foodbasket().name(),
                                   url_data_template=dgov_foodbasket().url_data_template,
                                   url_total=dgov_foodbasket().url_total,
                                   datasets=dgov_foodbasket().verisons)


if __name__ == '__main__':
    luigi.run()
