import os

import attr
import requests
import xmltodict
from box import Box

from kgd.exceptions import NetworkError
from kgd.utils import (read_file, requests_retry_session,
                   run_command, append_file)
from kgd.validators import common_corrector, date_corrector


def is_bin_processed(bn, processed_fpath):
    """ Check if BIN has been already processed """
    r = run_command(['grep', bn, processed_fpath])
    if r:
        return True

    return False


def processed_bins_fpath(fpath):
    """ Return path to file with processed BINs """
    return os.path.join(os.path.dirname(fpath),
                        os.path.basename(fpath) + '.prsd')


@attr.s
class PaymentData:
    """ Wrap dict and validate/convert values of each field"""
    bin = attr.ib(default='')
    taxorgcode = attr.ib(converter=common_corrector, default='')
    nametaxru = attr.ib(converter=common_corrector, default='')
    nametaxkz = attr.ib(converter=common_corrector, default='')
    kbk = attr.ib(converter=common_corrector, default='')
    kbknameru = attr.ib(converter=common_corrector, default='')
    kbknamekz = attr.ib(converter=common_corrector, default='')
    paynum = attr.ib(converter=common_corrector, default='')
    paytype = attr.ib(converter=common_corrector, default='')
    entrytype = attr.ib(converter=common_corrector, default='')
    receiptdate = attr.ib(converter=date_corrector, default='')
    writeoffdate = attr.ib(converter=date_corrector, default='')
    summa = attr.ib(converter=common_corrector, default='')


class TaxPaymentParser:

    request_template = read_file(
        os.path.join(os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.pardir)), 'request.xml'
        )
    )

    url_template = "http://{}/cits/api/v1/public/payments?token={}"
    headers = {'content-type': 'text/xml'}
    # 429 - too many requests
    status_forcelist = (429, 500, 502, 504)

    def __init__(self, fpath, fsize):
        self._output_files = []
        self._add_output_files(fpath, fsize)
        self._failed_bins_count = 0
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _curr_output_file(self):
        return self._output_files[len(self._output_files)-1]

    @property
    def fails(self):
        return self._failed_bins_count

    @property
    def output_file(self):
        return self._curr_output_file()

    def _add_output_files(self, fpath, fsize):
        """
        Gather all existed output files paths and
        if it's needed add new
        """
        self._output_files = []
        base = os.path.join(os.path.dirname(fpath),
                            os.path.splitext(os.path.basename(fpath))[0])

        output_path = f'{base}_out.csv'

        suffix = 2
        while os.path.exists(output_path):
            self._output_files.append(output_path)
            if os.path.getsize(fpath) < fsize:
                return
            output_path = f'{base}_out_{suffix}.csv'
            suffix += 1

        open(output_path, 'a').close()
        self._output_files.append(output_path)

    def process(self, bn, address_port=None, token=None,
                fpath=None, fsize=None, date_range=None,
                retries=None, backoff=None, timeout=None,
                reporthook=None):
        def csv_payment_row(p_dict):
            values = []
            #
            dct = {k.lower(): v.lower() for k, v in p_dict.items()}
            payment_data = PaymentData(**dct)
            return attr.astuple(payment_data)

        # size limit for output csv file
        if os.path.getsize(self._curr_output_file()) >= fsize:
            self._add_output_files(fpath)

        payments = []
        request = self.request_template.format(bn, *date_range)
        try:
            session = requests_retry_session(retries, backoff,
                                             status_forcelist=self.status_forcelist,
                                             session=self.session)
            response = session.post(self.url_template.format(address_port,
                                                             token),
                                    data=request,
                                    timeout=timeout)
        except Exception:
            self._failed_bins_count += 1
            raise NetworkError("Failed to process BIN")

        # Box for payments
        d = None

        # convert xml to json with OOP features
        if response:
            d = Box(xmltodict.parse(response.text)).answer

        # check if we got some logical errors in response
        if 'err' in d:
            return

        # it might be just one payment
        payments = d.payment if isinstance(d.payment, list) else [d.payment]

        rows = []

        # get only values and put them to list
        # according order how fields going in PaymentData class
        # build csv rows and write them to current output file
        for p in payments:
            p.bin = bn
            append_file(self.output_file, ';'.join(csv_payment_row(p)))
