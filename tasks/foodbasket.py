import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tasks.base import GzipToFtp, BaseConfig, ParseElasticApi, GzipDataGovToFtp
from tcomapi.common.utils import save_csvrows, append_file
from tcomapi.common.correctors import float_corrector
from tcomapi.common.data_verification import is_float
from tcomapi.dgov.api import (load_versions, load_data,
                              build_url_for_data_page, build_url_for_report_page)

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

        rep_url = build_url_for_report_page(self.rep_name)
        versions = self.versions
        if not versions:
            versions = load_versions(rep_url)
        for vs in versions:
            url = build_url_for_data_page(self.rep_name, DGOV_API_KEY, version=vs)
            data = load_data(url, Row)
            save_csvrows(self.output().path, data)


class FoodBasket(luigi.WrapperTask):

    def requires(self):
        return GzipDataGovToFtp(name='dgov_foodbasket',
                                versions=('v3', 'v4'),
                                rep_name='tabysy_azyk-tulik_korzhyny_kun',
                                struct=Row)


if __name__ == '__main__':
    luigi.run()
