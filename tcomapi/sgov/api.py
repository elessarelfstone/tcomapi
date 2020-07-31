import asyncio
import json

from collections import Counter
from os.path import basename
from http.client import responses
from time import sleep

import aiohttp
import attr
import requests
from aiohttp.http_exceptions import HttpProcessingError
from box import Box
from requests import ConnectionError, HTTPError, Timeout


from tcomapi.common.correctors import common_corrector, bool_corrector
from tcomapi.common.exceptions import ExternalSourceError
from tcomapi.common.data_file_helper import DataFileHelper
from tcomapi.common.ratelimit import Ratelimit
from tcomapi.common.utils import append_file, dict_to_csvrow, download


class SgovClientError(Exception):
    pass


class NotSuccessError(Exception):
    pass


class NotSuccessRCutError(Exception):
    pass


@attr.s
class JuridicalInfo:
    bin = attr.ib(converter=common_corrector, default='')
    ip = attr.ib(converter=bool_corrector, default='')
    name = attr.ib(converter=common_corrector, default='')
    fio = attr.ib(converter=common_corrector, default='')
    katocode = attr.ib(converter=common_corrector, default='')
    krpcode = attr.ib(converter=common_corrector, default='')
    okedcode = attr.ib(converter=common_corrector, default='')
    registerdate = attr.ib(converter=common_corrector, default='')
    secondokeds = attr.ib(converter=common_corrector, default='')
    katoaddress = attr.ib(converter=common_corrector, default='')
    okedname = attr.ib(converter=common_corrector, default='')
    krpbfcode = attr.ib(converter=common_corrector, default='')
    krpbfname = attr.ib(converter=common_corrector, default='')
    katoid = attr.ib(converter=common_corrector, default='')
    krpname = attr.ib(converter=common_corrector, default='')


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


class SgovRCutParser:
    host = 'stat.gov.kz'
    rcuts_url = 'https://{}/api/rcut/ru'.format(host)
    request_url = 'https://{}/api/sbr/request'.format(host)
    result_url_tmpl = 'https://{}/api/sbr/requestResult/{}/ru'
    rcut_download_url_tmpl = 'https://{}/api/sbr/download?bucket=SBR&guid={}'

    juridical_type_class_version_id = 2153
    status_class_version_id = 1989

    # could be changed
    statuses = [39354, 39355, 39356, 39358, 534829, 39359]

    def __init__(self, timeout=620):
        self.timeout = timeout
        try:
            r = requests.get(self.rcuts_url, verify=False)
            if r.status_code != 0:
                r.raise_for_status()
            rcuts = json.loads(r.text)
            self.curr_cut_id = rcuts[0]["id"]
        except (ConnectionError, HTTPError, Timeout) as e:
            # print(e)
            raise ExternalSourceError('Statgov rcuts not available')

    def get_order(self, juridical_type):
        """ Place order to get file
            klass: KATO identificator
        """
        # make request to put order to get file GUID
        jt = {"classVersionId": self.juridical_type_class_version_id, "itemIds": [juridical_type]}
        s = {"classVersionId": self.status_class_version_id, "itemIds": self.statuses}
        request = json.dumps({'conditions': [jt, s],
                              'stringForMD5': 'string',
                              'cutId': self.curr_cut_id})

        # return order number
        r = requests.post(self.request_url, headers=headers, data=request)
        print(r.json())
        return r.json()['obj']

    def get_file_guid(self, order_no):
        url = self.result_url_tmpl.format(self.host, order_no)
        r = requests.get(url, headers=headers)
        response = r.json()
        print(response)
        _guid = None
        if response.get('success') is True and response.get('description') == 'Обработан':
            _guid = response.get('obj', {}).get('fileGuid')
        else:
            raise NotSuccessRCutError('Rcut file guid not available.')

        return _guid

    def get_url(self, juridical_type):
        order_id = self.get_order(juridical_type)
        # we have to wait for while, cause order could be yet not ready
        sleep(self.timeout)
        rcut_guid = self.get_file_guid(order_id)
        sleep(10)
        return self.rcut_download_url_tmpl.format(self.host, rcut_guid)


class SgovJuridicalsParser:
    host = 'stat.gov.kz'
    url = 'https://{}/api/juridical/?bin={}&lang=ru'

    def __init__(self, fm, semaphore_limit=20, ratelimit=10):
        self.fm = fm
        self._not_success_list = []
        self.stat = Counter()
        for s in ['nse']:
            self.stat.setdefault(s, 0)
        self.ratelimit = Ratelimit(ratelimit)
        self.semaphore_limit = semaphore_limit

    def _status(self, idx, fname):
        curr = '{} in {}'.format(idx, fname)
        stata = ' '.join(f'{k}:{v}' for k, v in self.stat.items())
        return curr + ' ' + stata

    async def _load(self, session, idx):

        # prepare url with bin/iin
        url = self.url.format(self.host, idx)

        async with session.get(url) as r:
            status_code = r.status
            if status_code != 200:
                # we are doing something wrong
                if 400 <= status_code <= 499:
                    raise SgovClientError(f'{status_code} Client error : {responses[status_code]}')
                else:
                    raise HttpProcessingError(code=status_code,
                                              message=r.reason,
                                              headers=r.headers)
            else:
                _json = await r.json()
                d = Box(_json)
                if not d.success:
                    raise NotSuccessError()
                else:
                    return d.obj

    async def process_id(self, session, idx, semaphore):

        row = ()
        try:
            async with self.ratelimit:
                async with semaphore:
                    d = await self._load(session, idx)
                    row = ';'.join(dict_to_csvrow(d, JuridicalInfo))
        except NotSuccessError as e:
            self.stat['nse'] += 1
            # print('--', idx1)
            append_file(self.fm.parsed_file, idx)
        else:
            append_file(self.fm.curr_file, row)
            append_file(self.fm.parsed_file, idx)

        return row, idx

    async def _process_coro(self, ids, hook=None):
        semaphore = asyncio.Semaphore(self.semaphore_limit)
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            to_do = [self.process_id(session, _id, semaphore) for _id in ids]
            to_do_iter = asyncio.as_completed(to_do)

            for future in to_do_iter:
                try:
                    _, idx = await future
                    if hook:
                        hook(1, self._status(idx, basename(self.fm.curr_file)))

                except HttpProcessingError as e:
                    pass

                except Exception as e:
                    raise

    def process(self, ids, hook=None):
        loop = asyncio.get_event_loop()
        coro = self._process_coro(ids, hook=hook)
        loop.run_until_complete(coro)
        loop.close()
#
