import os
from codecs import encode
from datetime import datetime, timedelta

import attr
import luigi
from bs4 import BeautifulSoup as bs
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.utils import save_csvrows, get
from tcomapi.common.dates import DEFAULT_DATE_FORMAT
from tasks.addressregister import GzipElasticApiParsingToCsv
from tasks.base import GzipDataGovToFtp, GzipToFtp, LoadingDataIntoCsvFile
from tcomapi.dgov.api import load_versions, DatagovApiParsing, build_url_for_report_page
from tasks.elastic import ElasticApiParsing
from settings import TMP_DIR

FRMT = '%d.%m.%Y'


@attr.s
class Rate:
    kod = attr.ib(default='')
    name_kaz = attr.ib(default='')
    name_rus = attr.ib(default='')
    kurs = attr.ib(default='')
    date = attr.ib(default='')


@attr.s
class NbRate:
    code = attr.ib(default='')
    rate = attr.ib(default='')
    date = attr.ib(default='')


def nb_rates_as_csvrows(url):
    r = get(url)
    soup = bs(encode(r, encoding='utf-8'), 'lxml')
    table = soup.find('table')
    rows = table.find_all('tr')
    csvrows = []
    for tr in rows:
        values = tr.find_all('td')
        rate = values[-2].text
        code = values[-3].text.split('/')[0].strip()
        date = datetime.strftime(datetime.today(), DEFAULT_DATE_FORMAT)
        csvrows.append((code, rate, date))
    return csvrows


class DgovRatesParsing(LoadingDataIntoCsvFile, ElasticApiParsing):

    date = luigi.DateParameter(default=datetime.today())

    def run(self):
        url = build_url_for_report_page(self.report_name)
        version = load_versions(url)[-1]

        parser = DatagovApiParsing(self.api_key, self.report_name, self.struct,
                                   self.chunk_size, self.output().path)
        d = self.date.strftime(FRMT)
        q = '{"size":%s,"query":{"bool":{"must":[{"match":{"date":"%s"}}]}}}' % (self.chunk_size, d)

        data = parser.parse_query_report(version, q)
        save_csvrows(self.output().path, data)


@requires(DgovRatesParsing)
class GzipDgovRates(GzipToFtp):
    pass


class NbRatesParsing(LoadingDataIntoCsvFile):

    url = luigi.Parameter()

    def run(self):

        rows = nb_rates_as_csvrows(self.url)
        save_csvrows(self.output().path, rows)


@requires(NbRatesParsing)
class GzipNbRates(GzipToFtp):
    pass


class NbRates(luigi.WrapperTask):
    def requires(self):
        return GzipNbRates(name='nb_rates',
                           url='https://nationalbank.kz/ru/exchangerates/ezhednevnye-oficialnye-rynochnye-kursy-valyut',
                           directory=TMP_DIR,
                           struct=Rate)


class Rates(luigi.WrapperTask):
    def requires(self):
        # versions = load_versions('https://data.egov.kz/datasets/view?index=nbrk_currency_rates1')
        return GzipDgovRates(name='dgov_rates',
                             directory=TMP_DIR,
                             # date=datetime(year=2021, month=1, day=3),
                             date=datetime.today(),
                             versions=('v16',),
                             report_name='nbrk_currency_rates1',
                             struct=Rate)


if __name__ == '__main__':
    luigi.run()
