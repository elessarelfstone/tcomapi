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


class SgovRCutParser:
    host = 'stat.gov.kz'
    rcuts_url = 'https://{}/api/rcut/ru'.format(host)
    request_url_tmpl = 'https://{}/api/sbr/request'.format(host)
    check_req_url_tmpl = 'https://{}/api/sbr/requestResult/{}/{}'
    file_download_url_tmpl = 'https://{}/api/sbr/download?bucket=SBR&guid={}'

    # kato_id = 741880

    def __init__(self, conditions, wait_for_request_done=10):
        self.conditions = conditions
        self.wait_for_request_done = wait_for_request_done
        try:
            r = requests.get(self.rcuts_url, verify=False)
            if r.status_code != 0:
                r.raise_for_status()
            rcuts = json.loads(r.text)
            self.curr_cut_id = rcuts[0]["id"]
        except (ConnectionError, HTTPError, Timeout):
            raise ExternalSourceError('Statgov rcuts not available')

    def file_url(self, kato_ids):

        kato_condition = {"classVersionId": 213, "itemIds": kato_ids}
        conditions = self.conditions
        conditions.append(kato_condition)
        data = {"conditions": conditions, "cutId": self.curr_cut_id}

        r = requests.post(self.request_url_tmpl, json=data, verify=False)
        if r.status_code != 0:
            r.raise_for_status()

        rdata = json.loads(r.text)
        if rdata["success"]:
            obj_num = rdata["obj"]
        else:
            raise NotSuccessError('Request is not success')

        url = self.check_req_url_tmpl.format(self.host, obj_num, 'ru')
        file_guid = None
        while not file_guid:
            sleep(self.wait_for_request_done)
            r = requests.get(url, verify=False)
            rdata = r.json()
            if rdata["success"]:
                if rdata["description"] == 'Обработан':
                    file_guid = rdata["fileGuid"]
            else:
                raise NotSuccessError('Checking request is not success')
        return file_guid

    def parse(self):
        pass


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


# conditions = [{"classVerisonId": 2153, "itemIds": [742681]},
#               {"classVersionId": 1989, "itemIds": [39355]},
#             {"classVersionId": 213, "itemIds": [741880]}]

conditions = [{"classVerisonId": 2153, "itemIds": [742681]}, {"classVersionId": 1989, "itemIds": [39355]}]

obj = SgovRCutParser(conditions)
print(obj.file_url([253161]))

