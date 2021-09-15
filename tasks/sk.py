from time import sleep

import attr
import luigi
from box import Box

from luigi.util import requires
import requests
from requests.auth import HTTPBasicAuth


from settings import TMP_DIR, SK_USER, SK_PASSWORD, SK_TCOM_COMPANY_ID
from tasks.base import GzipToFtp, BigDataToCsv


from tcomapi.common.correctors import basic_corrector
from tcomapi.common.dates import today_as_str, DEFAULT_DATE_FORMAT
from tcomapi.common.utils import (dict_to_csvrow, save_csvrows, append_file)

SK_BASE_URL = 'https://integr.skc.kz/data/'


def default_corrector(value):
    if value is None:
        return ''
    return value


@attr.s
class SkSuppliers:
    id = attr.ib(converter=default_corrector, default='')
    identifier = attr.ib(converter=basic_corrector, default='')
    name_kk = attr.ib(converter=basic_corrector, default='')
    name_ru = attr.ib(converter=basic_corrector, default='')
    country_id = attr.ib(converter=default_corrector, default='')
    begin_date = attr.ib(converter=basic_corrector, default='')
    end_date = attr.ib(converter=basic_corrector, default='')
    first_name = attr.ib(converter=basic_corrector, default='')
    iin = attr.ib(converter=basic_corrector, default='')
    last_name = attr.ib(converter=basic_corrector, default='')
    middle_name = attr.ib(converter=basic_corrector, default='')
    company_id = attr.ib(converter=default_corrector, default='')
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
    postcode = attr.ib(converter=basic_corrector, default='')
    street = attr.ib(converter=basic_corrector, default='')
    kato_id = attr.ib(converter=default_corrector, default='')


@attr.s
class SkPurchases:
    id = attr.ib(converter=default_corrector, default='')
    row_number = attr.ib(converter=default_corrector, default='')
    plan_id = attr.ib(converter=default_corrector, default='')
    plan_item_group_id = attr.ib(converter=default_corrector, default='')
    count = attr.ib(converter=default_corrector, default='')
    price = attr.ib(converter=default_corrector, default='')
    ext_id = attr.ib(converter=default_corrector, default='')
    lot_id = attr.ib(converter=default_corrector, default='')
    advert_id = attr.ib(converter=default_corrector, default='')
    lot_count = attr.ib(converter=default_corrector, default='')
    lot_price = attr.ib(converter=default_corrector, default='')
    lot_sum_tru_nds = attr.ib(converter=default_corrector, default='')
    lot_sum_tru_no_nds = attr.ib(converter=default_corrector, default='')
    number = attr.ib(converter=default_corrector, default='')
    acceptance_begin_date_time = attr.ib(converter=default_corrector, default='')
    acceptance_end_date_time = attr.ib(converter=default_corrector, default='')
    name_ru = attr.ib(converter=default_corrector, default='')
    advert_tender_priority = attr.ib(converter=default_corrector, default='')
    jhi_comment = attr.ib(converter=default_corrector, default='')
    status = attr.ib(converter=default_corrector, default='')
    status_date_time = attr.ib(converter=default_corrector, default='')
    company_id = attr.ib(converter=default_corrector, default='')
    sum = attr.ib(converter=default_corrector, default='')
    advert_status = attr.ib(converter=default_corrector, default='')


@attr.s
class SkKztContracts:
    id = attr.ib(converter=default_corrector, default='')
    contract_id = attr.ib(converter=default_corrector, default='')
    created_date = attr.ib(converter=default_corrector, default='')
    contract_date_time = attr.ib(converter=default_corrector, default='')
    contract_number = attr.ib(converter=default_corrector, default='')
    control_date_time = attr.ib(converter=default_corrector, default='')
    foreign_sum = attr.ib(converter=default_corrector, default='')
    sum_nds = attr.ib(converter=default_corrector, default='')
    sum_no_nds = attr.ib(converter=default_corrector, default='')
    tender_type = attr.ib(converter=default_corrector, default='')
    currency_id = attr.ib(converter=default_corrector, default='')
    advert_id = attr.ib(converter=default_corrector, default='')
    flag_paper_contract = attr.ib(converter=default_corrector, default='')
    nds_size_id = attr.ib(converter=default_corrector, default='')
    contract_items_name_kk = attr.ib(converter=default_corrector, default='')
    contract_items_name_ru = attr.ib(converter=default_corrector, default='')
    local_content_projected_share = attr.ib(converter=default_corrector, default='')
    contract_type = attr.ib(converter=default_corrector, default='')
    prev_contract_card_id = attr.ib(converter=default_corrector, default='')
    main_contract_card_id = attr.ib(converter=default_corrector, default='')
    system_number = attr.ib(converter=default_corrector, default='')
    last_contract_card_id = attr.ib(converter=default_corrector, default='')
    rescission_date_time = attr.ib(converter=default_corrector, default='')
    rescission_type = attr.ib(converter=default_corrector, default='')
    currency_rate_id = attr.ib(converter=default_corrector, default='')
    currency_date = attr.ib(converter=default_corrector, default='')
    flag_nds_lot = attr.ib(converter=default_corrector, default='')
    jhi_comment = attr.ib(converter=default_corrector, default='')
    status = attr.ib(converter=default_corrector, default='')
    id_supplier = attr.ib(converter=default_corrector, default='')
    id_customer = attr.ib(converter=default_corrector, default='')
    supplier_bik = attr.ib(converter=default_corrector, default='')
    supplier_add_req = attr.ib(converter=default_corrector, default='')
    supplier_iik = attr.ib(converter=default_corrector, default='')
    supplier_bank_name_ru = attr.ib(converter=default_corrector, default='')


@attr.s
class SkBadSuppliers:
    bin_iin = attr.ib(converter=default_corrector, default='')
    name_ru = attr.ib(converter=default_corrector, default='')
    full_name_ru = attr.ib(converter=default_corrector, default='')
    reason_ru = attr.ib(converter=default_corrector, default='')
    created_date = attr.ib(converter=default_corrector, default='')
    start_date = attr.ib(converter=default_corrector, default='')
    end_date = attr.ib(converter=default_corrector, default='')


class SKAllRowsParsing(BigDataToCsv):

    uri = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    timeout = luigi.IntParameter(default=10)
    limit = luigi.IntParameter(default=100)
    login = luigi.Parameter(default='')

    add_par = luigi.Parameter(default='')

    def run(self):
        error_timeout = self.timeout * 3
        # headers = dict()
        # headers['Authorization'] = self.token
        url = f'{SK_BASE_URL}{self.uri}?{self.add_par}&size={self.limit}'
        if self.login:
            url += f'&login={self.login}'
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
                    url = f'{SK_BASE_URL}{self.uri}?{self.add_par}&size={self.limit}&page={page}'
                    if self.login:
                        url += f'&login={self.login}'
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
    login = luigi.Parameter(default='')

    add_par = luigi.Parameter(default='')

    def run(self):
        error_timeout = self.timeout * 3
        # headers = dict()
        # headers['Authorization'] = self.token

        url = f'{SK_BASE_URL}{self.uri}?{self.add_par}&size={self.limit}&after={self.after}'
        if self.login:
            url += f'&login={self.login}'
        page = 0
        total = 100
        parsed_count = 0
        while url:
            try:
                print(url)
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
                    url = f'{SK_BASE_URL}{self.uri}?{self.add_par}&size={self.limit}&after={self.after}&page={page}'
                    if self.login:
                        url += f'&login={self.login}'
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


class SkAllKztPurchasesToCsv(SKAllRowsParsing):
    pass


@requires(SkAllKztPurchasesToCsv)
class GzipSkAllKztPurchasesToCsv(GzipToFtp):
    pass


class SkAllKztContractsToCsv(SKAllRowsParsing):
    pass


@requires(SkAllKztContractsToCsv)
class GzipSkAllKztContractsToCsv(GzipToFtp):
    pass


@requires(SkAllKztContractsToCsv)
class GzipSkAllKztContractsToCsv(GzipToFtp):
    pass


class SkAllBadSuppliers(SKAllRowsParsing):
    pass


@requires(SkAllBadSuppliers)
class GzipSkAllBadSuppliers(GzipToFtp):
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


class SkAllKztPurchases(luigi.WrapperTask):
    def requires(self):
        return GzipSkAllKztPurchasesToCsv(
            directory=TMP_DIR,
            sep=';',
            uri='purchases/purchaseList',
            name='sk_kzt_purchases',
            struct=SkPurchases,
            user=SK_USER,
            password=SK_PASSWORD,
            login=SK_USER
        )


class SkKztAllContracts(luigi.WrapperTask):
    def requires(self):

        return GzipSkAllKztContractsToCsv(
            directory=TMP_DIR,
            sep=';',
            uri='contract/contractList',
            add_par=f'companyIdentifier={SK_TCOM_COMPANY_ID}',
            name='sk_kzt_contracts',
            struct=SkKztContracts,
            user=SK_USER,
            password=SK_PASSWORD,
            login=SK_USER
        )


class SkKztAllBadSuppliers(luigi.WrapperTask):
    def requires(self):
        return GzipSkAllBadSuppliers(
            directory=TMP_DIR,
            sep=';',
            uri='bad-supplier/badSupplierList',
            name='sk_bad_suppliers',
            struct=SkBadSuppliers,
            user=SK_USER,
            password=SK_PASSWORD,
            login=SK_USER
        )


class SkSuppliersForDateToCsv(SKAfterDateRowsParsing):
    pass


@requires(SkSuppliersForDateToCsv)
class GzipSkSuppliersForDateToCsv(GzipToFtp):
    pass


class SkKztPurchasesForDateToCsv(SKAfterDateRowsParsing):
    pass


@requires(SkKztPurchasesForDateToCsv)
class GzipSkKztPurchasesForDateToCsv(GzipToFtp):
    pass


class SkAllKztContractsForDateToCsv(SKAfterDateRowsParsing):
    pass


@requires(SkAllKztContractsForDateToCsv)
class GzipSkAllKztContractsForDateToCsv(GzipToFtp):
    pass


class SkBadSuppliersForDateToCsv(SKAfterDateRowsParsing):
    pass


@requires(SkBadSuppliersForDateToCsv)
class GzipSkBadSuppliersForDateToCsv(GzipToFtp):
    pass


class SkSuppliersForDate(luigi.WrapperTask):

    after = luigi.Parameter(default=today_as_str(dt_format=DEFAULT_DATE_FORMAT))
    # after = luigi.Parameter(default='2021-07-05')

    def requires(self):
        return GzipSkSuppliersForDateToCsv(
            directory=TMP_DIR,
            ftp_directory='samruk',
            after=self.after,
            sep=';',
            uri='suppliers/supplierList',
            name='sk_suppliers',
            struct=SkSuppliers,
            user=SK_USER,
            password=SK_PASSWORD
        )


class SkKztPurchasesForDate(luigi.WrapperTask):

    after = luigi.Parameter(default=today_as_str(dt_format=DEFAULT_DATE_FORMAT))
    # after = luigi.Parameter(default='2021-07-05')

    def requires(self):
        return GzipSkKztPurchasesForDateToCsv(
            directory=TMP_DIR,
            ftp_directory='samruk',
            after=self.after,
            sep=';',
            uri='purchases/purchaseList',
            name='sk_kzt_purchases',
            struct=SkPurchases,
            user=SK_USER,
            password=SK_PASSWORD,
            login=SK_USER
        )


class SkKztContractsForDate(luigi.WrapperTask):

    after = luigi.Parameter(default=today_as_str(dt_format=DEFAULT_DATE_FORMAT))

    def requires(self):
        return GzipSkAllKztContractsForDateToCsv(
            directory=TMP_DIR,
            ftp_directory='samruk',
            after=self.after,
            sep=';',
            uri='contract/contractList',
            add_par=f'companyIdentifier={SK_TCOM_COMPANY_ID}',
            name='sk_kzt_contracts',
            struct=SkKztContracts,
            user=SK_USER,
            password=SK_PASSWORD
        )


class SkBadSuppliersForDate(luigi.WrapperTask):

    after = luigi.Parameter(default=today_as_str(dt_format=DEFAULT_DATE_FORMAT))

    def requires(self):
        return GzipSkBadSuppliersForDateToCsv(
            directory=TMP_DIR,
            ftp_directory='samruk',
            after=self.after,
            sep=';',
            uri='bad-supplier/badSupplierList',
            name='sk_bad_suppliers',
            struct=SkBadSuppliers,
            user=SK_USER,
            password=SK_PASSWORD
        )


if __name__ == '__main__':
    luigi.run()
