import json
import os
from datetime import date, datetime, timedelta
import fnmatch
from os.path import basename, exists, join

import attr
import luigi
from calendar import monthrange
from luigi.contrib.ftp import RemoteTarget, RemoteFileSystem
from luigi.util import requires

from settings import (FTP_IN_PATH, FTP_HOST, FTP_PATH,
                      FTP_PASS, FTP_USER, BIGDATA_TMP_DIR)
from tasks.base import BigDataToCsv, GzipToFtp
from tcomapi.kgd.api import KgdTaxPaymentParser, KgdServerNotAvailableError

from settings import KGD_API_TOKEN
from tcomapi.common.constants import SERVER_IS_DOWN
from tcomapi.common.correctors import basic_corrector, date_corrector, num_corrector
from tcomapi.common.dates import previous_month_as_str, month_as_range
from tcomapi.common.exceptions import NoBinsToParseTaxPayments
from tcomapi.common.dataflow import last_file_with_bins
from tcomapi.common.utils import (build_fpath, append_file, gziped_fname,
                                  date_for_fname, gzip_file)

IN_FILENAME = 'kgd.bins'
IN_FILENAME_TMPL = 'export_kgdgovkz_bins'


BINS_REMOTE_DIR = 'export'


@attr.s
class TaxPaymentsRow:
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


class KgdBins(luigi.ExternalTask):

    bins_fname_tmp = 'export_kgdgovkz_bins_*.csv'

    def output(self):
        rmfs = RemoteFileSystem(FTP_HOST, username=FTP_USER, password=FTP_PASS)

        files = None

        if rmfs.exists(FTP_IN_PATH):
            lst = rmfs.listdir(FTP_IN_PATH)
            files = fnmatch.filter([basename(l) for l in lst], self.bins_fname_tmp)
        else:
            NoBinsToParseTaxPayments('Could not find directory with bins')

        if not files:
            raise NoBinsToParseTaxPayments('Could not find any file with bins')

        # bins_fpath = join(FTP_IN_PATH, last_file_with_bins(files))
        bins_fpath = FTP_IN_PATH + '/' + last_file_with_bins(files)
        return RemoteTarget(bins_fpath, FTP_HOST,
                            username=FTP_USER, password=FTP_PASS)


class KgdTaxPaymentsParsing(BigDataToCsv):

    # month = luigi.Parameter(default=default_month())

    notaxes_fext = luigi.Parameter('notaxes')
    start_date = luigi.Parameter()
    end_date = luigi.Parameter()
    timeout = luigi.FloatParameter(default=1.5)

    @property
    def notaxes_fpath(self):
        return build_fpath(self.directory, self.name, self.notaxes_fext)

    def requires(self):
        return KgdBins()

    def output(self):

        # return luigi.LocalTarget(build_fpath(BIGDATA_TMP_DIR, self.name, 'csv'))
        return [luigi.LocalTarget(self.parsed_fpath),
                luigi.LocalTarget(self.notaxes_fpath),
                # TODO check this out
                super().output()
                # luigi.LocalTarget(build_fpath(self.directory, self.name, 'csv')),
                ]

    def run(self):
        def percent(total, parsed):
            return round((parsed * 100) / total)

        bids_fpath = build_fpath(self.directory, self.name, 'bins')

        # copy on local machine from ftp
        if not exists(bids_fpath):
            self.input().get(bids_fpath)

        date_range = self.start_date, self.end_date

        parser = KgdTaxPaymentParser(self.name, bids_fpath, date_range,
                                     KGD_API_TOKEN, self.struct, self.timeout)

        # until the last bid
        while parser.bids:
            # if we have failed bids
            # pull last to process again
            if parser.failed_bids:
                bid = parser.failed_bids.popleft()
                r = True
            else:
                bid = parser.bids.popleft()
                r = False

            # refresh status bar
            status = parser.status(bid, r)
            self.set_status_message(status)

            try:
                r = parser.process_bin(bid)
                self.set_progress_percentage(percent(parser.source_bids_count,
                                                     parser.parsed_bids_count))

            except KgdServerNotAvailableError:
                print(SERVER_IS_DOWN)
                exit()

            except Exception as e:
                print(e)
                raise

        stata = dict(total=parser.source_bids_count,
                     parsed_count=parser.parsed_bids_count)

        append_file(parser.success_fpath, json.dumps(stata))


@requires(KgdTaxPaymentsParsing)
class GzipKgdTaxPaymentsToFtp(luigi.Task):

    def output(self):
        # fix from 30.07.2020
        # requested by Esmukhanov to avoid probability getting no data for DataFlow
        # here we just use tomorrow date to build output file name
        dt = datetime.today() + timedelta(days=1)

        ftp_prs_gzip_fpath = os.path.join(FTP_PATH, gziped_fname(self.input()[0].path,
                                                                 suff=date_for_fname(dt)))
        ftp_notax_gzip_fpath = os.path.join(FTP_PATH, gziped_fname(self.input()[1].path,
                                                                   suff=date_for_fname(dt)))
        ftp_data_gzip_fpath = os.path.join(FTP_PATH, gziped_fname(self.input()[2].path,
                                                                  suff=date_for_fname(dt)))

        return [
            RemoteTarget(ftp_prs_gzip_fpath, FTP_HOST,
                         username=FTP_USER, password=FTP_PASS),
            RemoteTarget(ftp_notax_gzip_fpath, FTP_HOST,
                         username=FTP_USER, password=FTP_PASS),
            RemoteTarget(ftp_data_gzip_fpath, FTP_HOST,
                         username=FTP_USER, password=FTP_PASS)
        ]

    def run(self):
        for i, f in enumerate(self.input()):
            _fpath = gzip_file(f.path)
            self.output()[i].put(_fpath, atomic=False)


# @requires(KgdTaxPaymentsParsing)
# class GzipKgdTaxPaymentsToFtp(GzipToFtp):
#     pass


class KgdTaxPaymentsForMonth(luigi.WrapperTask):
    """  As result we should get three files:
        payments data in .csv file, all processed bins in .prs file and
        bins with no payments data for given month
    """
    month = luigi.Parameter(default=previous_month_as_str())

    def requires(self):
        # year = int(self.month[:4])
        # month = int(self.month[-2:])
        # start_date = date(year, month, 1).strftime('%Y-%m-%d')
        # end_date = date(year, month, monthrange(year, month)[1]).strftime('%Y-%m-%d')

        start_date, end_date = month_as_range(self.month)

        yield GzipKgdTaxPaymentsToFtp(start_date=start_date.strftime('%Y-%m-%d'),
                                      end_date=end_date.strftime('%Y-%m-%d'),
                                      name='kgd_taxpayments',
                                      timeout=2,
                                      struct=TaxPaymentsRow,
                                      directory=BIGDATA_TMP_DIR)


class KgdTaxPaymentsForPeriod(luigi.WrapperTask):

    start_date = luigi.Parameter()
    end_date = luigi.Parameter()

    def requires(self):
        yield GzipKgdTaxPaymentsToFtp(start_date=self.start_date,
                                      end_date=self.end_date,
                                      name='kgd_taxpayments',
                                      timeout=2)


if __name__ == '__main__':
    luigi.run()

