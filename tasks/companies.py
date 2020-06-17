import re
import os
import urllib3
from os.path import basename
# from urllib.parse import urlparse
from urllib.parse import urlsplit
from shutil import move

import attr
import luigi
import requests
from bs4 import BeautifulSoup
from luigi.configuration.core import add_config_path
from luigi.util import requires
from requests_html import HTMLSession

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_csvrows, save_webfile, build_fpath
from tcomapi.common.unpacking import unpack, zipo_flist
from settings import CONFIG_DIR, TMP_DIR
from tasks.base import GzipToFtp, BaseConfig

config_path = os.path.join(CONFIG_DIR, 'companies.conf')
add_config_path(config_path)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@attr.s
class Row:
    bin = attr.ib(default='')
    full_name_kz = attr.ib(default='')
    full_name_ru = attr.ib(default='')
    registration_date = attr.ib(default='')
    oked_1 = attr.ib(default='')
    activity_kz = attr.ib(default='')
    activity_ru = attr.ib(default='')
    oked_2 = attr.ib(default='')
    krp = attr.ib(default='')
    krp_name_kz = attr.ib(default='')
    krp_name_ru = attr.ib(default='')
    kato = attr.ib(default='')
    settlement_kz = attr.ib(default='')
    settlement_ru = attr.ib(default='')
    legal_address = attr.ib(default='')
    head_fio = attr.ib(default='')


class sgov_companies(BaseConfig):
    url = luigi.Parameter(default=None)
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')
    sheets = luigi.TupleParameter(default=None)


class RetrieveCompaniesWebDataFiles(luigi.Task):
    url = luigi.Parameter(default=None)
    name = luigi.Parameter(default=None)

    def _links(self):
        """ Parse links for all regions"""

        # parse page with list of urls to files
        sess = HTMLSession()
        resp = sess.get(self.url)
        resp.html.render()

        soup = BeautifulSoup(resp.html.html, "lxml")

        links = []
        for li in soup.find_all('ul')[7]:
            link = "{0.scheme}://{0.netloc}/{1}".format(urlsplit(self.url), li.find('a').get('href'))
            links.append(link)

        return links

    def output(self):
        fpaths = []
        links = self._links()

        # according number of links we build paths for
        # xls files, cause each zip file contains one xls file
        for i, link in enumerate(links):
            fpath = os.path.join(TMP_DIR, f'{self.name}_{i}.xls')
            fpaths.append(fpath)

        return [luigi.LocalTarget(f) for f in fpaths]

    def run(self):
        links = self._links()
        for i, f in enumerate(self.output()):
            # set status
            self.set_status_message('Saving {}'.format(basename(f.path)))

            save_webfile(links[i], f.path)

            percent = round((i+1)*100/len(self.output()))
            self.set_progress_percentage(percent)


@requires(RetrieveCompaniesWebDataFiles)
class ParseCompanies(luigi.Task):

    sheets = luigi.TupleParameter(default=None)

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))

    def run(self):
        for i, target in enumerate(self.input()):
            self.set_status_message('Parsing {}'.format(target.path))
            sheets = self.sheets
            rows = parse(target.path, Row, skiprows=sgov_companies().skiptop,
                         sheets=sheets)
            save_csvrows(self.output().path, [attr.astuple(r) for r in rows])

            percent = round((i + 1) * 100 / len(self.input()))
            self.set_progress_percentage(percent)


@requires(ParseCompanies)
class GzipCompaniesToFtp(GzipToFtp):
    pass


class Companies(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesToFtp(url=sgov_companies().url,
                                 name=sgov_companies().name(),
                                 sheets=sgov_companies().sheets)


if __name__ == '__main__':
    luigi.run()
