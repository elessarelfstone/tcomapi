import os
from datetime import date
from os.path import basename, exists

import luigi
from luigi.contrib.ftp import RemoteTarget
from luigi.util import requires

from settings import FTP_IN_PATH, FTP_HOST, FTP_PASS, FTP_USER, TMP_DIR
from tasks.base import ParseBigData, GzipToFtp, prev_month, month_to_range
from tcomapi.kgd.api import KgdTaxPaymentParser, KgdServerNotAvailableError

from settings import KGD_API_TOKEN
from tcomapi.common.constants import SERVER_IS_DOWN
from tcomapi.common.utils import build_fpath, append_file


IN_FILENAME = 'kgd.bins'


def default_month() -> str:
    """ Usually we parse taxpayments for previous month"""
    today = date.today()
    _month = (today.year, today.month)
    _prev_year, _prev_month = prev_month(_month)
    return '{}-{}'.format(today.year, today.month)


class KgdBins(luigi.ExternalTask):
    def output(self):
        bins_ftp_path = os.path.join(FTP_IN_PATH, 'bins.csv')
        return RemoteTarget(bins_ftp_path, FTP_HOST,
                            username=FTP_USER, password=FTP_PASS)


class ParseKgdTaxPayments(ParseBigData):

    # month = luigi.Parameter(default=default_month())
    start_date = luigi.Parameter()
    end_date = luigi.Parameter()
    timeout = luigi.FloatParameter(default=1.5)

    def requires(self):
        return KgdBins()

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))

    def run(self):
        def percent(total, parsed):
            return round((parsed * 100) / total)

        bids_fpath = build_fpath(TMP_DIR, self.name, 'bins')

        if not exists(bids_fpath):
            self.input().get(bids_fpath)

        # date_range = month_to_range(self.month)
        date_range = self.start_date, self.end_date

        parser = KgdTaxPaymentParser(self.name, bids_fpath, date_range,
                                     KGD_API_TOKEN, self.timeout)

        total_count = parser.source_bids_count
        parsed_count = parser.parsed_bids_count

        while parser.bids:
            incr = 1
            if parser.failed_bids:
                bid = parser.failed_bids.popleft()
                incr = 0
                r = True
            else:
                bid = parser.bids.popleft()
                r = True
            # refresh status bar
            fname = basename(parser.output)
            status = parser.status(parsed_count, bid, r)
            self.set_status_message(status)
            try:
                r = parser.process_bin(bid)
                parsed_count += incr
                self.set_progress_percentage(percent(total_count,
                                                     parsed_count))

            except KgdServerNotAvailableError:
                print(SERVER_IS_DOWN)
                exit()

            except Exception as e:
                print(e)
                raise

        stata = dict(total=total_count, parsed_count=parsed_count)
        append_file(parser.result_fpath, stata)


@requires(ParseKgdTaxPayments)
class GzipKgdTaxPaymentsToFtp(GzipToFtp):
    pass


class KgdTaxPaymentsForMonth(luigi.WrapperTask):

    month = luigi.Parameter()

    def requires(self):
        yield GzipKgdTaxPaymentsToFtp(month=self.month,
                                      name='kgd_taxpayments',
                                      timeout=2)


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
