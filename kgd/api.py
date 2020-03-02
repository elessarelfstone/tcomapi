import os
import attr
import urllib3
from collections import deque, Counter
from http.client import responses
from time import sleep

import requests
from requests import HTTPError, ConnectionError
from box import Box
from xmltodict import parse
from xml.parsers.expat import ExpatError

# from common import ParseFilesManager
from common.utils import is_server_up, append_file, read_file, prepare

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SEP = ';'


class KgdClientError(Exception):
    pass


class KgdResponseError(Exception):
    """ Connection, network, 5XX errors, etc"""
    pass


class KgdRequestError(Exception):
    """ KGD requests count limitation,
    html or some other trash not xml formatted"""
    pass


def basic_corrector(value):
    return value.rstrip().replace('"', "'").replace('\n', '')


def sep_clean(value):
    return value.replace(SEP, '')


def date_corrector(value):
    # return common_corrector(value).split('+')[0]
    return value.split('+')[0]


def num_corrector(value):
    return sep_clean(basic_corrector(value))


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


class KgdTaxPaymentParser:
    request_template = read_file(
        os.path.join(os.path.abspath(
            os.path.join(os.path.dirname(__file__))), 'request.xml'
        )
    )
    host = 'data.egov.kz'
    url_template = "https://{}/proxy2/culs_payments?token={}"
    headers = {'user-agent': 'Apache-HttpClient/4.1.1 (java 1.5)',
               'content-type': 'text/xml'}

    def __init__(self, token, timeout):
        self.token = token
        self.timeout = timeout
        self._failed_bins = deque([])
        self.stat = Counter()
        for s in ['rqe', 'rse', 'se', 's']:
            self.stat.setdefault(s, 0)
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    @property
    def failed(self):
        return self._failed_bins

    def load(self, _bin, date_range):
        request = self.request_template.format(_bin, *date_range)
        url = self.url_template.format(self.host, self.token)

        r = requests.post(url, request, headers=self.headers, verify=False)

        status_code = r.status_code

        if status_code != 200:
            # we are doing something wrong
            if 400 <= status_code <= 499:
                raise KgdClientError(f'{status_code} Client error : {responses[status_code]}')
            else:
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
            p.bin = _bin

        return [prepare(p, PaymentData) for p in payments]

    def process_bin(self, _bin, date_range, out_fpath, prs_fpath):

        try:
            payments = self.load(_bin, date_range)

        except KgdRequestError:
            # we are done with this bin
            append_file(prs_fpath, _bin)
            self.stat['rqe'] += 1

        except KgdResponseError:
            # just mark _bin as failed and sleep
            self._failed_bins.append(_bin)
            self.stat['rse'] += 1
            sleep(self.timeout)

        except (HTTPError, ConnectionError) as e:
            self.stat['se'] += 1
            sleep(self.timeout)
            # continue if service is available
            if is_server_up(self.host):
                self._failed_bins.append(_bin)
            else:
                return -1

        else:
            # write payments to output file
            for p in payments:
                append_file(out_fpath, SEP.join(p))
            # write bin to prs file
            append_file(prs_fpath, _bin)
            self.stat['s'] += 1

        return 1

    @staticmethod
    def status(c, _bin, fname, reprocess):
        if reprocess:
            r = 'R'
        else:
            r = ''
        curr = '{} in {} {}'.format(_bin, fname, r)
        stata = ' '.join(f'{k}:{v}' for k, v in c.items())

        return curr + ' ' + stata
