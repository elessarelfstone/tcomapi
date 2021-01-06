import os
from datetime import datetime, timedelta

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tasks.addressregister import GzipElasticApiParsingToCsv
from tasks.base import GzipDataGovToFtp, GzipToFtp
from tcomapi.dgov.api import load_versions
from tasks.elastic import BigDataElasticApiParsingToCsv, DATE_FORMAT
from tcomapi.common.dates import previous_month_as_str, month_as_dates_range


@attr.s
class Rate:
    kod = attr.ib(default='')
    name_kaz = attr.ib(default='')
    name_rus = attr.ib(default='')
    kurs = attr.ib(default='')
    date = attr.ib(default='')


class Rates(luigi.WrapperTask):

    def requires(self):
        # versions = load_versions('https://data.egov.kz/datasets/view?index=nbrk_currency_rates1')
        return GzipElasticApiParsingToCsv(name='dgov_rates',
                                          versions=('v16',),
                                          report_name='nbrk_currency_rates1',
                                          struct=Rate)


if __name__ == '__main__':
    luigi.run()
