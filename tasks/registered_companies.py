import re
import json
import os
import urllib3
from time import sleep
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

from tcomapi.common.excel import parse_to_csv
from tcomapi.common.utils import (save_csvrows, save_webfile,
                                  build_fpath, fname_noext, fpath_noext)
from tcomapi.common.unpacking import unpack, zipo_flist, unzip_one_file
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


headers = {
    'authority': 'stat.gov.kz',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'accept': 'application/json, text/plain, */*',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36',
    'content-type': 'application/json;charset=UTF-8',
    'origin': 'https://stat.gov.kz',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://stat.gov.kz/jur-search/filter',
    'accept-language': 'ru,en-US;q=0.9,en;q=0.8',
}


class RetrieveZipsWithRegCompanies(luigi.Task):
    name = luigi.Parameter()
    url_rcut_list = "https://stat.gov.kz/api/rcut/ru"
    url_request_order = "https://stat.gov.kz/api/sbr/request"
    url_request_result_tmpl = "https://stat.gov.kz/api/sbr/requestResult/{}/ru"
    url_download_tmpl = "https://stat.gov.kz/api/sbr/download?bucket=SBR&guid={}"

    def _get_last_rcut(self):
        r = requests.get(self.url_rcut_list)
        id_rcut = r.json()[0]['id']
        name_rcut = r.json()[0]['name']
        return id_rcut, name_rcut.split()[1]

    def _get_order_ids(self, cut_id):
        juridical_type_items_ids = [742679, 742680, 742681, 742684, 742687]
        status_ids = [39354, 39355, 39356, 39358, 534829, 39359]

        juridical_type_condition = {"classVersionId": 2153, "itemIds": juridical_type_items_ids}
        status_condition = {"classVersionId": 1989, "itemIds": status_ids}

        _ids = []

        for jt in juridical_type_items_ids:
            juridical_type_condition = {"classVersionId": 2153, "itemIds": [jt]}
            request = {"conditions": [juridical_type_condition, status_condition], "stringForMD5": "string"}
            request.update({"cutId": cut_id})
            data = json.dumps(request)
            r = requests.post(self.url_request_order, headers=headers, data=data)
            order_no = r.json()['obj']
            _ids.append(order_no)

        return _ids

    def _file_guids(self, order_nos):
        f_guids = []
        for ono in order_nos:
            _url = self.url_request_result_tmpl.format(ono)
            r = requests.get(_url)
            d = r.json()
            if d.get('success') is True and d.get('description') == 'Обработан':
                f_guids.append(d.get('obj', {}).get('fileGuid'))

        return f_guids

    def _get_download_urls(self):
        rcut, _ = self._get_last_rcut()
        orders = self._get_order_ids(rcut)
        if orders:
            sleep(10)

        f_guids = self._file_guids(orders)
        return [self.url_download_tmpl.format(fg) for fg in f_guids]

    def output(self):
        f_paths = []
        if not hasattr(self, '_urls_to_download'):
            self._urls_to_download = self._get_download_urls()
        for i, url, in enumerate(self._urls_to_download):
            f_p = build_fpath(TMP_DIR, f'{self.name}_{i}', 'xlsx')
            f_paths.append(f_p)

        return [luigi.LocalTarget(fp) for fp in f_paths]

    def run(self):
        for i, f in enumerate(self.output()):
            self.set_status_message('Downloding ...'.format())
            arch_fpath = fpath_noext(f.path)
            frmt = save_webfile(self._urls_to_download[i], arch_fpath)
            name = f'{self.name}_{i}'
            fpath = unzip_one_file(arch_fpath, name)


@requires(RetrieveZipsWithRegCompanies)
class ParseRegCompanies(luigi.Task):

    skiprows = luigi.TupleParameter(default=None)

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))

    def run(self):
        for i, target in enumerate(self.input()):
            self.set_status_message('Parsing {}'.format(target.path))
            parse_to_csv(target.path, self.output().path, Row, skiprows=self.skiprows)
            percent = round((i + 1) * 100 / len(self.input()))
            self.set_progress_percentage(percent)


@requires(ParseRegCompanies)
class GzipRegCompaniesToFtp(GzipToFtp):
    pass


class RegCompanies(luigi.WrapperTask):
    def requires(self):
        yield GzipRegCompaniesToFtp(name='sgov_regcompanies',
                                    skiprows=2)


if __name__ == '__main__':
    luigi.run()
