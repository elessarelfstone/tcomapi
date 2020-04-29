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


@attr.s
class Row:
    oblrus = attr.ib(default='')
    оblkaz = attr.ib(default='')
    year = attr.ib(default='')
    edizmrus = attr.ib(default='')
    edizmkaz = attr.ib(default='')
    vsego = attr.ib(default='')


class dgov_incomepopul(BaseConfig):
    rep_name = luigi.Parameter(default='')
    url_total = luigi.Parameter(default='')
    versions = luigi.TupleParameter(default=tuple())


config_path = os.path.join(CONFIG_DIR, 'incomepopul.conf')
add_config_path(config_path)


class ParseIncomePopul(ParseElasticApi):

    def run(self):
        rep_url = report_url(self.rep_name)
        versions = self.versions
        if not versions:
            versions = load_versions(rep_url)
        for vs in versions:
            url = data_url(self.rep_name, DGOV_API_KEY, version=vs)
            data = load_data_as_tuple(url, Row)
            save_to_csv(self.output().path, data)


@requires(ParseIncomePopul)
class GzipFoodBasketToFtp(GzipToFtp):
    pass


class IncomePopul(luigi.WrapperTask):

    def requires(self):
        return GzipFoodBasketToFtp(name=dgov_incomepopul().name(),
                                   versions=dgov_incomepopul().versions,
                                   rep_name=dgov_incomepopul().rep_name)


if __name__ == '__main__':
    luigi.run()
