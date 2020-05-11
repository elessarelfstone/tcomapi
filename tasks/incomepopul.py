import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tasks.base import GzipToFtp, BaseConfig, ParseElasticApi, GzipDataGovToFtp
from tcomapi.common.utils import save_csvrows, append_file
from tcomapi.common.correctors import float_corrector
from tcomapi.common.data_verification import is_float
from tcomapi.dgov.api import (load_versions, build_url_data, build_url_report)

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


class IncomePopul(luigi.WrapperTask):

    def requires(self):
        return GzipDataGovToFtp(name='dgov_incomepopul',
                                versions=('v3', 'v4'),
                                rep_name='halyktyn_ortasha_zhan_basyna_s16',
                                struct=Row)


if __name__ == '__main__':
    luigi.run()
