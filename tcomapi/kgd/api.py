import os
import attr
import urllib3
from collections import deque, Counter
from http.client import responses
from time import sleep
from urllib3.exceptions import ProtocolError

import requests
from http.client import RemoteDisconnected
from requests import HTTPError, ConnectionError, ReadTimeout
from box import Box
from xmltodict import parse
from xml.parsers.expat import ExpatError

# from common import ParseFilesManager
from tcomapi.common.constants import CSV_SEP
from tcomapi.common.correctors import basic_corrector, date_corrector, num_corrector
from tcomapi.common.parsers import BidsBigDataToCsvHandler
from tcomapi.common.utils import is_server_up, append_file, read_file, dict_to_csvrow

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


KGD_TIMEOUT_FACTOR = 10


class KgdServerNotAvailableError(Exception):
    pass


class KgdClientError(Exception):
    pass


class KgdResponseError(Exception):
    """ Connection, network, 5XX errors, etc"""
    pass


class KgdTooManyRequests(Exception):
    pass


class KgdRequestError(Exception):
    """ KGD requests count limitation,
    html or some other trash not xml formatted"""
    pass


@attr.s
class PaymentData:
    """ Wrap structure provides convenient way to handle payment data.
      Such as validating, converting etc"""
    bin = attr.ib(default='')
    taxorgcode = attr.ib(converter=basic_corrector, default='')
    nametaxru = attr.ib(converter=basic_corrector, default='')
    nametaxkz = attr.ib(converter=basic_corrector, default='')
    kbk = attr.ib(converter=basic_corrector, default='')
    kbknameru = attr.ib(converter=basic_corrector, default='')
    kbknamekz = attr.ib(converter=basic_corrector, default='')
    paynum = attr.ib(converter=num_corrector, default='')
    paytype = attr.ib(converter=basic_corrector, default='')
    entrytype = attr.ib(converter=basic_corrector, default='')
    receiptdate = attr.ib(converter=date_corrector, default='')
    writeoffdate = attr.ib(converter=date_corrector, default='')
    summa = attr.ib(converter=num_corrector, default='')


class KgdTaxPaymentParser(BidsBigDataToCsvHandler):
    request_template = read_file(
        os.path.join(os.path.abspath(
            os.path.join(os.path.dirname(__file__))), 'request.xml'
        )
    )
    host = 'data.egov.kz'
    url_template = "https://{}/proxy2/culs_payments?token={}"
    headers = {'user-agent': 'Apache-HttpClient/4.1.1 (java 1.5)',
               'content-type': 'text/xml'}

    def __init__(self, name, bids_fpath, date_range,
                 token, timeout, limit_outputfsize=None):
        super().__init__(name, bids_fpath, limit_outputfsize)
        self._token = token
        self._timeout = timeout
        self._date_range = date_range
        self._stat = Counter()
        for s in ['rqe', 'rse', 'se', 's']:
            self._stat.setdefault(s, 0)
        self._session = requests.Session()
        self._session.headers.update(self.headers)

    def _load(self, bid):
        request = self.request_template.format(bid, *self._date_range)
        url = self.url_template.format(self.host, self._token)

        r = requests.post(url, request, headers=self.headers, verify=False, timeout=self._timeout)

        status_code = r.status_code

        if status_code != 200:
            if status_code == 429:
                raise KgdTooManyRequests('Kgd limitation exceeded')
            r.raise_for_status()

        if r.text:
            try:
                d = Box(parse(r.text)).answer

            # it could be html(from squid proxy for example)
            # or some other trash
            # not xml formatted
            except ExpatError:
                raise KgdResponseError('Not XML formatted')

        else:
            # recently KGD started to send
            # empty response to limit
            # count of requests in certain time
            # earlier they've just sending TooManyRequests exception
            raise KgdResponseError('Empty response received')

        # if we get request error
        # something wrong with our xml-request
        # all errors described in KGD API docs
        if 'err' in d:
            errcode = d.err.errorcode
            raise KgdRequestError(f'Errorcode {errcode}')

        # it might be just one payment
        payments = d.payment if isinstance(d.payment, list) else [d.payment]

        # enrich each row by bin
        for p in payments:
            p.bin = bid

        return [dict_to_csvrow(p, PaymentData) for p in payments]

    def process_bin(self, bid):

        payments = []

        try:
            payments = self._load(bid)

        except KgdRequestError:
            # we are done with this bin
            # self._failed_bins.append(_bin)
            append_file(self._parsed_fpath, bid)
            self._stat['rqe'] += 1
            sleep(self._timeout)

        except KgdResponseError:
            # just mark _bin as failed and sleep
            self._failed_bids.append(bid)
            self._stat['rse'] += 1
            sleep(self._timeout)

        except (HTTPError, ConnectionError, ReadTimeout,
                KgdTooManyRequests, RemoteDisconnected, ProtocolError) as e:
            self._stat['se'] += 1
            sleep(self._timeout * KGD_TIMEOUT_FACTOR)
            # continue if service is available
            if is_server_up(self.host):
                self._failed_bids.append(bid)
            else:
                raise KgdServerNotAvailableError('Kgd is not available')

        else:
            # write payments to output file
            for p in payments:
                append_file(self.output, CSV_SEP.join(p))
            # write bin to prs file
            append_file(self._parsed_fpath, bid)
            self._stat['s'] += 1
            sleep(1.5)

        return payments

    def status(self, bid, reprocess=False):
        if reprocess:
            r = 'R'
        else:
            r = ''
        curr = '{} in {} {}'.format(bid, self.output, r)
        stata = ' '.join(f'{k}:{v}' for k, v in self._stat.items())

        return curr + ' ' + stata
