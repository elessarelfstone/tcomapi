import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tasks.base import GzipToFtp, BaseConfig, GzipDataGovToFtp
from tcomapi.common.utils import save_csvrows, append_file
from tcomapi.common.correctors import float_corrector
from tcomapi.common.data_verification import is_float
from tcomapi.dgov.api import (load_versions, build_url_for_data_page, build_url_for_report_page)

from settings import CONFIG_DIR, DGOV_API_KEY


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


class UnemplRate(luigi.WrapperTask):

    def requires(self):
        return GzipDataGovToFtp(name='dgov_unemplrate',
                                versions=('v7', 'v8', 'v9', 'v10', 'v11'),
                                rep_name='zhumyssyzdyk_dengeii1',
                                struct=Row)


if __name__ == '__main__':
    luigi.run()
