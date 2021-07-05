import attr
import luigi
from box import Box

from luigi.util import requires
import requests
from requests.auth import HTTPBasicAuth
from time import sleep

from settings import TMP_DIR, SK_USER, SK_PASSWORD
from tasks.base import GzipToFtp, BigDataToCsv


from tcomapi.common.correctors import basic_corrector
from tcomapi.common.dates import today_as_str, DEFAULT_DATE_FORMAT
from tcomapi.common.utils import (dict_to_csvrow, save_csvrows, append_file)

BASE_URL = 'https://integr.skc.kz/data/'


@attr.s
class SkSuppliers:
    id = attr.ib(default='')
    identifier = attr.ib(default='')
    name_kk = attr.ib(converter=basic_corrector, default='')
    name_ru = attr.ib(converter=basic_corrector, default='')
    country_id = attr.ib(default='')
    begin_date = attr.ib(default='')
    end_date = attr.ib(default='')
    first_name = attr.ib(converter=basic_corrector, default='')
    iin = attr.ib(default='')
    last_name = attr.ib(converter=basic_corrector, default='')
    middle_name = attr.ib(converter=basic_corrector, default='')
    company_id = attr.ib(default='')
    position_kk = attr.ib(converter=basic_corrector, default='')
    position_ru = attr.ib(converter=basic_corrector, default='')
    email = attr.ib(converter=basic_corrector, default='')
    fax = attr.ib(converter=basic_corrector, default='')
    full_name_kk = attr.ib(converter=basic_corrector, default='')
    full_name_ru = attr.ib(converter=basic_corrector, default='')
    phone = attr.ib(converter=basic_corrector, default='')
    site = attr.ib(converter=basic_corrector, default='')
    building = attr.ib(converter=basic_corrector, default='')
    city = attr.ib(converter=basic_corrector, default='')
    flat = attr.ib(converter=basic_corrector, default='')
    postcode = attr.ib(default='')
    street = attr.ib(converter=basic_corrector, default='')
    kato_id = attr.ib(default='')


class SKAllRowsParsing(BigDataToCsv):

    uri = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    timeout = luigi.IntParameter(default=10)
    limit = luigi.IntParameter(default=100)

    def run(self):
        error_timeout = self.timeout * 3
        # headers = dict()
        # headers['Authorization'] = self.token

        url = f'{BASE_URL}{self.uri}?size={self.limit}'
        page = 0
        total = 0
        parsed_count = 0
        while url:
            try:
                r = requests.get(url, timeout=self.timeout,
                                 auth=HTTPBasicAuth(self.user, self.password))
            except Exception:
                sleep(error_timeout)
            else:
                response = Box(r.json())
                if response.content:
                    page += 1
                    url = f'{BASE_URL}{self.uri}?size={self.limit}&page={page}'
                else:
                    url = None

                total = response.totalElements
                raw_items = response.content
                # data = dict_to_csvrow(raw_items, self.struct)
                data = [dict_to_csvrow(d, self.struct) for d in raw_items]
                save_csvrows(self.output().path, data)
                parsed_count += len(raw_items)
                sleep(self.timeout)

            self.set_status_message(f'Total: {total}. Parsed: {parsed_count}')
            self.set_progress_percentage(round((parsed_count * 100)/total))

        stat = dict(total=total, parsed=parsed_count)
        append_file(self.success_fpath, str(stat))


class SKAfterDateRowsParsing(BigDataToCsv):

    uri = luigi.Parameter()
    after = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    timeout = luigi.IntParameter(default=10)
    limit = luigi.IntParameter(default=100)

    def run(self):
        error_timeout = self.timeout * 3
        # headers = dict()
        # headers['Authorization'] = self.token

        url = f'{BASE_URL}{self.uri}?size={self.limit}&after={self.after}'
        page = 0
        total = 100
        parsed_count = 0
        while url:
            try:
                r = requests.get(url, timeout=self.timeout,
                                 auth=HTTPBasicAuth(self.user, self.password))
            except Exception:
                sleep(error_timeout)
            else:
                response = Box(r.json())

                raw_items = response.content
                # data = dict_to_csvrow(raw_items, self.struct)
                data = [dict_to_csvrow(d, self.struct) for d in raw_items]
                save_csvrows(self.output().path, data)
                parsed_count += len(raw_items)

                if response.totalElements > parsed_count:
                    page += 1
                    url = f'{BASE_URL}{self.uri}?size={self.limit}&after={self.after}&page={page}'
                else:
                    url = None

                sleep(self.timeout)

        stat = dict(total=total, parsed=parsed_count)
        append_file(self.success_fpath, str(stat))


class SkAllSuppliersToCsv(SKAllRowsParsing):
    pass


@requires(SkAllSuppliersToCsv)
class GzipSkAllSuppliersToCsv(GzipToFtp):
    pass


class SkAllSuppliers(luigi.WrapperTask):
    def requires(self):
        return GzipSkAllSuppliersToCsv(
            directory=TMP_DIR,
            sep=';',
            uri='suppliers/supplierList',
            name='sk_suppliers',
            struct=SkSuppliers,
            user=SK_USER,
            password=SK_PASSWORD
        )


class SkSuppliersForDateToCsv(SKAfterDateRowsParsing):
    pass


@requires(SkSuppliersForDateToCsv)
class GzipSkSuppliersForDateToCsv(GzipToFtp):
    pass


class SkSuppliersForDate(luigi.WrapperTask):

    after = luigi.Parameter(default=today_as_str(dt_format=DEFAULT_DATE_FORMAT))

    def requires(self):
        return GzipSkSuppliersForDateToCsv(
            directory=TMP_DIR,
            after=self.after,
            sep=';',
            uri='suppliers/supplierList',
            name='sk_suppliers',
            struct=SkSuppliers,
            user=SK_USER,
            password=SK_PASSWORD
        )


if __name__ == '__main__':
    luigi.run()
