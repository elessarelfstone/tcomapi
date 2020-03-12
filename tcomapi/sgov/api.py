import asyncio
from collections import Counter
from os.path import basename
from http.client import responses

import aiohttp
import attr
from box import Box
from aiohttp.http_exceptions import HttpProcessingError

from tcomapi.common.correctors import common_corrector, bool_corrector
from tcomapi.common.ratelimit import Ratelimit
from tcomapi.common.utils import append_file, prepare


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


class SgovJuridicalsParser:
    host = 'stat.gov.kz'
    url = 'https://{}/api/juridical/?bin={}&lang=ru'

    def __init__(self, fpath, semaphore_limit=20, ratelimit=10):
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

    async def process_id(self, session, idx, fpath, prs_fpath, semaphore):

        row = ()
        try:
            async with self.ratelimit:
                async with semaphore:
                    d = await self._load(session, idx)
                    row = ';'.join(prepare(d, JuridicalInfo))
        except NotSuccessError as e:
            self.stat['nse'] += 1
            # print('--', idx1)
            append_file(prs_fpath, idx)
        else:
            append_file(fpath, row)
            append_file(prs_fpath, idx)

        return row, idx

    async def _process_coro(self, ids, fpath, prs_fpath, hook=None):
        semaphore = asyncio.Semaphore(self.semaphore_limit)
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            to_do = [self.process_id(session, _id, fpath, prs_fpath, semaphore) for _id in ids]
            to_do_iter = asyncio.as_completed(to_do)

            for future in to_do_iter:
                try:
                    _, idx = await future
                    if hook:
                        hook(1, self._status(idx, basename(fpath)))

                except HttpProcessingError as e:
                    pass

                except Exception as e:
                    raise

    def process(self, ids, fpath, prs_fpath, hook=None):
        loop = asyncio.get_event_loop()
        coro = self._process_coro(ids, fpath, prs_fpath, hook=hook)
        loop.run_until_complete(coro)
        loop.close()
