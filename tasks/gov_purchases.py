import json
import os
import pandas as pd
from urllib.parse import urlparse
from math import floor


import attr
import luigi
from box import Box
from gql import gql
from luigi.util import requires
from time import sleep

from settings import TMP_DIR, BIGDATA_TMP_DIR, GOSZAKUP_GQL_TOKEN, GOSZAKUP_REST_TOKEN
from tasks.base import GzipToFtp, LoadingDataIntoCsvFile, BigDataToCsv
from tasks.grql import GraphQlParsing
from tcomapi.common.dates import previous_date_as_str
from tcomapi.common.utils import (dict_to_csvrow, save_csvrows,
                                  get, read_lines,
                                  append_file, get_file_lines_count2)


@attr.s
class GoszakupCompanyRow:
    pid = attr.ib(default='')
    bin = attr.ib(default='')
    iin = attr.ib(default='')
    inn = attr.ib(default='')
    unp = attr.ib(default='')
    regdate = attr.ib(default='')
    crdate = attr.ib(default='')
    number_reg = attr.ib(default='')
    series = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')
    email = attr.ib(default='')
    phone = attr.ib(default='')
    website = attr.ib(default='')
    last_update_date = attr.ib(default='')
    country_code = attr.ib(default='')
    qvazi = attr.ib(default='')
    customer = attr.ib(default='')
    organizer = attr.ib(default='')
    mark_national_company = attr.ib(default='')
    ref_kopf_code = attr.ib(default='')
    mark_assoc_with_disab = attr.ib(default='')
    year = attr.ib(default='')
    mark_resident = attr.ib(default='')
    system_id = attr.ib(default='')
    supplier = attr.ib(default='')
    krp_code = attr.ib(default='')
    oked_list = attr.ib(default='')
    kse_code = attr.ib(default='')
    mark_world_company = attr.ib(default='')
    mark_state_monopoly = attr.ib(default='')
    mark_natural_monopoly = attr.ib(default='')
    mark_patronymic_producer = attr.ib(default='')
    mark_patronymic_supplyer = attr.ib(default='')
    mark_small_employer = attr.ib(default='')
    type_supplier = attr.ib(default='')
    is_single_org = attr.ib(default='')
    index_date = attr.ib(default='')


@attr.s
class GoszakupContractRow:
    id = attr.ib(default='')
    parent_id = attr.ib(default='')
    root_id = attr.ib(default='')
    trd_buy_id = attr.ib(default='')
    trd_buy_number_anno = attr.ib(default='')
    trd_buy_name_ru = attr.ib(default='')
    trd_buy_name_kz = attr.ib(default='')
    ref_contract_status_id = attr.ib(default='')
    deleted = attr.ib(default='')
    crdate = attr.ib(default='')
    last_update_date = attr.ib(default='')
    supplier_id = attr.ib(default='')
    supplier_biin = attr.ib(default='')
    supplier_bik = attr.ib(default='')
    supplier_iik = attr.ib(default='')
    supplier_bank_name_kz = attr.ib(default='')
    supplier_bank_name_ru = attr.ib(default='')
    supplier_legal_address = attr.ib(default='')
    supplier_bill_id = attr.ib(default='')
    contract_number = attr.ib(default='')
    sign_reason_doc_name = attr.ib(default='')
    sign_reason_doc_date = attr.ib(default='')
    trd_buy_itogi_date_public = attr.ib(default='')
    customer_id = attr.ib(default='')
    customer_bin = attr.ib(default='')
    customer_bik = attr.ib(default='')
    customer_iik = attr.ib(default='')
    customer_bill_id = attr.ib(default='')
    customer_bank_name_kz = attr.ib(default='')
    customer_bank_name_ru = attr.ib(default='')
    customer_legal_address = attr.ib(default='')
    contract_number_sys = attr.ib(default='')
    payments_terms_ru = attr.ib(default='')
    payments_terms_kz = attr.ib(default='')
    ref_subject_type_id = attr.ib(default='')
    ref_subject_types_id = attr.ib(default='')
    is_gu = attr.ib(default='') # integer
    fin_year = attr.ib(default='') # integer
    ref_contract_agr_form_id = attr.ib(default='')
    ref_contract_year_type_id = attr.ib(default='')
    ref_finsource_id = attr.ib(default='')
    ref_currency_code = attr.ib(default='')
    exchange_rate = attr.ib(default='')
    contract_sum = attr.ib(default='')
    contract_sum_wnds = attr.ib(default='')
    sign_date = attr.ib(default='')
    ec_end_date = attr.ib(default='')
    plan_exec_date = attr.ib(default='')
    fakt_exec_date = attr.ib(default='')
    fakt_sum = attr.ib(default='')
    fakt_sum_wnds = attr.ib(default='')
    contract_end_date = attr.ib(default='')
    ref_contract_cancel_id = attr.ib(default='')
    ref_contract_type_id = attr.ib(default='')
    description_kz = attr.ib(default='')
    description_ru = attr.ib(default='')
    fakt_trade_methods_id = attr.ib(default='')
    ec_customer_approve = attr.ib(default='')
    ec_supplier_approve = attr.ib(default='')
    contract_ms = attr.ib(default='')
    treasure_req_num = attr.ib(default='')
    treasure_req_date = attr.ib(default='')
    treasure_not_num = attr.ib(default='')
    treasure_not_date = attr.ib(default='')
    system_id = attr.ib(default='')
    index_date = attr.ib(default='')


@attr.s
class GoszakupUntrustedSupplierRow:
    pid = attr.ib(default='')
    supplier_biin = attr.ib(default='')
    supplier_innunp = attr.ib(default='')
    supplier_name_ru = attr.ib(default='')
    supplier_name_kz = attr.ib(default='')
    kato_list = attr.ib(default='')
    index_date = attr.ib(default='')
    system_id = attr.ib(default='')


@attr.s
class GoszakupContractTypeRow:
    id = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')


@attr.s
class GoszakupContractStatusRow:
    id = attr.ib(default='')
    code = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')


@attr.s
class GoszakupTenderMethodRow:
    code = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')
    is_active = attr.ib(default='')
    type = attr.ib(default='')
    symbol_code = attr.ib(default='')
    f1 = attr.ib(default='')
    ord = attr.ib(default='')
    f2 = attr.ib(default='')
    id = attr.ib(default='')


@attr.s
class GoszakupLotsStatusRow:
    id = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')
    code = attr.ib(default='')


@attr.s
class GoszakupBuyStatusRow:
    id = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')
    code = attr.ib(default='')


@attr.s
class GoszakupLotsRow:
    id = attr.ib(default='')
    lot_number = attr.ib(default='')
    ref_lot_status_id = attr.ib(default='')
    last_update_date = attr.ib(default='')
    union_lots = attr.ib(default='')
    count = attr.ib(default='')
    amount = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')
    description_ru = attr.ib(default='')
    description_kz = attr.ib(default='')
    customer_id = attr.ib(default='')
    customer_bin = attr.ib(default='')
    trd_buy_number_anno = attr.ib(default='')
    trd_buy_id = attr.ib(default='')
    dumping = attr.ib(default='')
    dumping_lot_price = attr.ib(default='')
    psd_sign = attr.ib(default='')
    consulting_services = attr.ib(default='')
    point_list = attr.ib(default='')
    singl_org_sign = attr.ib(default='')
    is_light_industry = attr.ib(default='')
    is_construction_work = attr.ib(default='')
    disable_person_id = attr.ib(default='')
    customer_name_kz = attr.ib(default='')
    customer_name_ru = attr.ib(default='')
    ref_trade_methods_id = attr.ib(default='')
    index_date = attr.ib(default='')
    system_id = attr.ib(default='')


@attr.s
class GoszakupTradeBuyRow:
    id = attr.ib(default='')
    number_anno = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')
    total_sum  = attr.ib(default='')
    count_lots = attr.ib(default='')
    ref_trade_methods_id = attr.ib(default='')
    ref_subject_type_id = attr.ib(default='')
    customer_bin = attr.ib(default='')
    customer_pid = attr.ib(default='')
    org_bin = attr.ib(default='')
    org_pid = attr.ib(default='')
    ref_buy_status_id = attr.ib(default='')
    start_date = attr.ib(default='')
    repeat_start_date = attr.ib(default='')
    repeat_end_date = attr.ib(default='')
    end_date = attr.ib(default='')
    publish_date = attr.ib(default='')
    itogi_date_public = attr.ib(default='')
    ref_type_trade_id = attr.ib(default='')
    disable_person_id = attr.ib(default='')
    discus_start_date = attr.ib(default='')
    discus_end_date = attr.ib(default='')
    id_supplier = attr.ib(default='')
    biin_supplier = attr.ib(default='')
    parent_id = attr.ib(default='')
    singl_org_sign = attr.ib(default='')
    is_light_industry = attr.ib(default='')
    is_construction_work = attr.ib(default='')
    customer_name_kz = attr.ib(default='')
    customer_name_ru = attr.ib(default='')
    org_name_kz = attr.ib(default='')
    org_name_ru = attr.ib(default='')
    system_id = attr.ib(default='')
    index_date = attr.ib(default='')


@attr.s
class GoszakupPlanRow:
    pid = attr.ib(default='')
    bin = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')
    doc_count = attr.ib(default='')


@attr.s
class GoszakupPlanPointRow:
    id = attr.ib(default='')
    rootrecord_id = attr.ib(default='')
    sys_subjects_id = attr.ib(default='')
    sys_organizator_id = attr.ib(default='')
    subject_biin = attr.ib(default='')
    subject_name_ru = attr.ib(default='')
    subject_name_kz = attr.ib(default='')
    name_ru = attr.ib(default='')
    name_kz = attr.ib(default='')
    ref_trade_methods_id = attr.ib(default='')
    ref_units_code = attr.ib(default='')
    count = attr.ib(default='')
    price = attr.ib(default='')
    amount = attr.ib(default='')
    ref_months_id = attr.ib(default='')
    ref_pln_point_status_id = attr.ib(default='')
    pln_point_year = attr.ib(default='')
    ref_subject_type_id = attr.ib(default='')
    ref_enstru_code = attr.ib(default='')
    ref_finsource_id = attr.ib(default='')
    ref_abp_code = attr.ib(default='')
    is_qvazi = attr.ib(default='')
    date_create = attr.ib(default='')
    timestamp = attr.ib(default='')
    ref_point_type_id = attr.ib(default='')
    desc_ru = attr.ib(default='')
    desc_kz = attr.ib(default='')
    extra_desc_kz = attr.ib(default='')
    extra_desc_ru = attr.ib(default='')
    sum_1 = attr.ib(default='')
    sum_2 = attr.ib(default='')
    sum_3 = attr.ib(default='')
    supply_date_ru = attr.ib(default='')
    prepayment = attr.ib(default='')
    ref_justification_id = attr.ib(default='')
    ref_amendment_agreem_type_id = attr.ib(default='')
    ref_amendm_agreem_justif_id = attr.ib(default='')
    contract_prev_point_id = attr.ib(default='')
    disable_person_id = attr.ib(default='')
    transfer_sys_subjects_id = attr.ib(default='')
    transfer_type = attr.ib(default='')
    ref_budget_type_id = attr.ib(default='')
    createdin_act_id = attr.ib(default='')
    is_active = attr.ib(default='')
    active_act_id = attr.ib(default='')
    is_deleted = attr.ib(default='')
    system_id = attr.ib(default='')
    index_date = attr.ib(default='')
    plan_act_id = attr.ib(default='')
    plan_act_number = attr.ib(default='')
    ref_plan_status_id = attr.ib(default='')
    plan_fin_year = attr.ib(default='')
    plan_preliminary = attr.ib(default='')
    date_approved = attr.ib(default='')


@attr.s
class GoszakupPlanKatoRow:
    id = attr.ib(default='')
    pln_points_id = attr.ib(default='')
    ref_kato_code = attr.ib(default='')
    ref_countries_code = attr.ib(default='')
    full_delivery_place_name_ru = attr.ib(default='')
    full_delivery_place_name_kz = attr.ib(default='')
    count = attr.ib(default='')
    is_active = attr.ib(default='')
    is_deleted = attr.ib(default='')
    system_id = attr.ib(default='')
    index_date = attr.ib(default='')


@attr.s
class GoszakupContractUnitsRow:
    contract_id = attr.ib(default='')
    id = attr.ib(default='')
    lot_id = attr.ib(default='')
    pln_point_id = attr.ib(default='')
    item_price = attr.ib(default='')
    item_price_wnds = attr.ib(default='')
    quantity = attr.ib(default='')
    total_sum = attr.ib(default='')
    total_sum_wnds = attr.ib(default='')
    fact_sum = attr.ib(default='')
    fact_sum_wnds = attr.ib(default='')
    ks_proc = attr.ib(default='')
    ks_sum = attr.ib(default='')
    deleted = attr.ib(default='')
    trd_buy_id = attr.ib(default='')
    contract_registry_id = attr.ib(default='')
    crdate = attr.ib(default='')
    exec_fakt_date = attr.ib(default='')
    exec_plan_date = attr.ib(default='')
    executed = attr.ib(default='')
    parent_id = attr.ib(default='')
    root_id = attr.ib(default='')
    ref_contract_status_id = attr.ib(default='')
    cr_deleted = attr.ib(default='')
    ref_amendm_agreem_justif_id = attr.ib(default='')
    system_id = attr.ib(default='')
    index_date = attr.ib(default='')


@attr.s
class GoszakupTrdAppOffersRow:
    id = attr.ib(default='')
    lot_id = attr.ib(default='')
    app_lot_id = attr.ib(default='')
    price = attr.ib(default='')
    amount = attr.ib(default='')
    system_id = attr.ib(default='')
    index_date = attr.ib(default='')
    # app_lot_point_list = attr.ib(default='')
    app_lot_status_id = attr.ib(default='')
    app_lot_price = attr.ib(default='')
    app_lot_amount = attr.ib(default='')
    app_lot_discount_value = attr.ib(default='')
    app_lot_discount_price = attr.ib(default='')
    app_lot_system_id = attr.ib(default='')
    app_lot_index_date = attr.ib(default='')
    app_id = attr.ib(default='')
    app_buy_id = attr.ib(default='')
    app_supplier_id = attr.ib(default='')
    app_cr_fio = attr.ib(default='')
    app_mod_fio = attr.ib(default='')
    app_supplier_bin_iin = attr.ib(default='')
    app_prot_id = attr.ib(default='')
    app_prot_number = attr.ib(default='')
    app_date_apply = attr.ib(default='')
    app_system_id = attr.ib(default='')
    app_index_date = attr.ib(default='')


def get_total(url: str, headers: str):
    r = get(url, headers=headers)
    return Box(json.loads(r)).total


def norm(d):
    df = pd.json_normalize(d, sep='')
    _norm = df.to_dict(orient='records')[0]
    return {k.lstrip('_'): v for k, v in _norm.items()}


def flatten_data(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)

    pat = '_0123456789'
    return out


def clean(d):
    pat = '_0123456789'
    return {k.lstrip(pat).rstrip(pat): v for k, v in d.items()}


def transform_nested_gql_response(entity, items):

    flatten = []
    for wrapper in items:
        d = wrapper[entity]
        if d:
            flatten.extend(d)

    return flatten


class GoszakupAllRowsParsing(BigDataToCsv, LoadingDataIntoCsvFile):

    url = luigi.Parameter()
    token = luigi.Parameter(default=GOSZAKUP_REST_TOKEN)
    timeout = luigi.IntParameter(default=1)
    limit = luigi.IntParameter(default=500)

    def run(self):
        error_timeout = self.timeout * 3
        headers = dict()
        headers['Authorization'] = self.token

        url = f'{self.url}?limit={self.limit}'
        host = '{uri.scheme}://{uri.netloc}'.format(uri=urlparse(url))

        # we store parsed blocks of data as uris
        # in case reruning we parse last uri
        if os.path.exists(self.parsed_fpath):
            uri = read_lines(self.parsed_fpath).pop()
            url = f'{host}{uri}'

        total = 0
        parsed_count = 0
        if os.path.exists(self.output().path):
            parsed_count = get_file_lines_count2(self.output().path)
        # parsed_count = 0 if not parsed_count else parsed_count

        while url:
            try:
                r = get(url, headers=headers, timeout=self.timeout)
            except Exception:
                sleep(error_timeout)
            else:
                response = Box(json.loads(r))
                if response.next_page:
                    url = f'{self.url}?{response.next_page}'
                    append_file(self.parsed_fpath, response.next_page)
                else:
                    url = None

                total = response.total
                raw_items = list(response['items'])
                # data = dict_to_csvrow(raw_items, self.struct)
                data = [dict_to_csvrow(d, self.struct) for d in raw_items]
                save_csvrows(self.output().path, data, quoter="\"")
                parsed_count += self.limit
                sleep(self.timeout)

            self.set_status_message(f'Total: {total}. Parsed: {parsed_count}')
            self.set_progress_percentage(round((parsed_count * 100)/total))

        stat = dict(total=total, parsed=parsed_count)
        append_file(self.success_fpath, str(stat))


class GoszakupCompaniesAllParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupCompaniesAllParsingToCsv)
class GzipGoszakupCompaniesAllParsingToCsv(GzipToFtp):
    pass


class GoszakupCompaniesAll(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupCompaniesAllParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                    sep=';',
                                                    url='https://ows.goszakup.gov.kz/v3/subject/all',
                                                    name='goszakup_companies',
                                                    struct=GoszakupCompanyRow)


class GoszakupContractsAllParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupContractsAllParsingToCsv)
class GzipGoszakupContractsAllParsingToCsv(GzipToFtp):
    pass


class GoszakupContractsAll(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupContractsAllParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                    sep=';',
                                                    url='https://ows.goszakup.gov.kz/v3/contract/all',
                                                    name='goszakup_contracts',
                                                    struct=GoszakupContractRow)


class GoszakupLotsAllParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupLotsAllParsingToCsv)
class GzipGoszakupLotsAllParsingToCsv(GzipToFtp):
    pass


class GoszakupLotsAll(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupLotsAllParsingToCsv(directory=BIGDATA_TMP_DIR,
                                               sep=';',
                                               url='https://ows.goszakup.gov.kz/v3/lots',
                                               name='goszakup_all_lots',
                                               struct=GoszakupLotsRow)


class GoszakupUntrustedSuppliersAllParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupUntrustedSuppliersAllParsingToCsv)
class GzipGoszakupUntrustedSuppliersAllParsingToCsv(GzipToFtp):
    pass


class GoszakupUntrustedSuppliersAll(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupUntrustedSuppliersAllParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                             ftp_directory='goszakup',
                                                             sep=';',
                                                             url='https://ows.goszakup.gov.kz/v3/rnu',
                                                             name='goszakup_untrusted',
                                                             monthly=True,
                                                             struct=GoszakupUntrustedSupplierRow)


class GoszakupContractTypesParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupContractTypesParsingToCsv)
class GzipGoszakupContractTypesParsingToCsv(GzipToFtp):
    pass


class GzipGoszakupContractTypes(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupContractTypesParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                     ftp_directory='goszakup',
                                                     sep=';',
                                                     url='https://ows.goszakup.gov.kz/v2/refs/ref_contract_type',
                                                     name='goszakup_contract_type',
                                                     monthly=True,
                                                     struct=GoszakupContractTypeRow)


class GoszakupContractStatusesParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupContractStatusesParsingToCsv)
class GzipGoszakupContractStatusesParsingToCsv(GzipToFtp):
    pass


class GzipGoszakupContractStatuses(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupContractStatusesParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                        ftp_directory='goszakup',
                                                        sep=';',
                                                        url='https://ows.goszakup.gov.kz/v2/refs/ref_contract_status',
                                                        name='goszakup_contract_status',
                                                        monthly=True,
                                                        struct=GoszakupContractStatusRow)


class GoszakupTradeMethodsParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupTradeMethodsParsingToCsv)
class GzipGoszakupTradeMethodsParsingToCsv(GzipToFtp):
    pass


class GoszakupTradeMethods(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupTradeMethodsParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                    ftp_directory='goszakup',
                                                    sep=';',
                                                    url='https://ows.goszakup.gov.kz/v2/refs/ref_trade_methods',
                                                    name='goszakup_trade_methods',
                                                    monthly=True,
                                                    struct=GoszakupTenderMethodRow)


class GoszakupLotsStatusParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupLotsStatusParsingToCsv)
class GzipGoszakupLotsStatusParsingToCsv(GzipToFtp):
    pass


class GoszakupLotsStatus(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupLotsStatusParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                  ftp_directory='goszakup',
                                                  sep=';',
                                                  url='https://ows.goszakup.gov.kz/v3/refs/ref_lots_status',
                                                  name='goszakup_lots_status',
                                                  monthly=True,
                                                  struct=GoszakupLotsStatusRow)


class GoszakupTradeBuysStatusParsingToCsv(GoszakupAllRowsParsing):
    #
    pass


@requires(GoszakupTradeBuysStatusParsingToCsv)
class GzipGoszakupTradeBuysParsingToCsv(GzipToFtp):
    pass


class GoszakupTradeBuysAll(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupTradeBuysParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                 ftp_directory='goszakup',
                                                 sep=';',
                                                 url='https://ows.goszakup.gov.kz/v3/trd-buy/all',
                                                 name='goszakup_trdbuy',
                                                 monthly=True,
                                                 struct=GoszakupTradeBuyRow)


class GoszakupPlanPointsAllParsingToCsv(GoszakupAllRowsParsing):
    #
    pass


@requires(GoszakupPlanPointsAllParsingToCsv)
class GzipGoszakupPlanPointsAllParsingToCsv(GzipToFtp):
    pass


class GoszakupPlanPointsAll(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupPlanPointsAllParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                     ftp_directory='goszakup',
                                                     sep=';',
                                                     url='https://ows.goszakup.gov.kz/v3/plans/all',
                                                     name='goszakup_plan_point',
                                                     monthly=True,
                                                     timeout=0.5,
                                                     struct=GoszakupPlanPointRow)


class GoszakupPlansStatusParsingToCsv(GoszakupAllRowsParsing):
    #
    pass


@requires(GoszakupPlansStatusParsingToCsv)
class GzipGoszakupPlansParsingToCsv(GzipToFtp):
    pass


class GoszakupPlansAll(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupPlansParsingToCsv(directory=BIGDATA_TMP_DIR,
                                             # ftp_directory='goszakup',
                                             sep=';',
                                             url='https://ows.goszakup.gov.kz/v3/plans',
                                             name='goszakup_plans',
                                             monthly=True,
                                             struct=GoszakupPlanRow)


class GoszakupBuyStatusParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupBuyStatusParsingToCsv)
class GzipGoszakupBuyStatusParsingToCsv(GzipToFtp):
    pass


class GoszakupBuyStatus(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupBuyStatusParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                 ftp_directory='goszakup',
                                                 sep=';',
                                                 url='https://ows.goszakup.gov.kz/v3/refs/ref_buy_status',
                                                 name='goszakup_buy_status',
                                                 monthly=True,
                                                 struct=GoszakupBuyStatusRow)


class GoszakupPlanKatoAllParsingToCsv(GoszakupAllRowsParsing):
    pass


@requires(GoszakupPlanKatoAllParsingToCsv)
class GzipGoszakupPlanKatoAllParsingToCsv(GzipToFtp):
    pass


class GoszakupPlanKatoAll(luigi.WrapperTask):
    def requires(self):
        return GzipGoszakupPlanKatoAllParsingToCsv(directory=BIGDATA_TMP_DIR,
                                                   ftp_directory='goszakup',
                                                   sep=';',
                                                   url='https://ows.goszakup.gov.kz/v3/plans/kato',
                                                   name='goszakup_plan_kato',
                                                   monthly=True,
                                                   struct=GoszakupPlanKatoRow)


class GoszakupGqlParsingToCsv(GraphQlParsing):
    entity = luigi.Parameter()
    start_date = luigi.Parameter(default=previous_date_as_str(1))
    end_date = luigi.Parameter(default=previous_date_as_str(1))
    limit = luigi.IntParameter(default=200)
    anchor_field = luigi.Parameter(default='id')

    def run(self):
        parsed_count = 0
        client = self.get_client()
        query = gql(self.query)
        start_from = None
        params = {'from': str(self.start_date), 'to': str(self.end_date), 'limit': self.limit}

        super().run()
        while True:
            p = params
            if start_from:
                p["after"] = start_from

            data = client.execute(query, variable_values=p, get_execution_result=True)
            extensions = data.extensions
            data = data.data
            entity, wrapper = self.entity, None
            if '_' in self.entity:
                entity, wrapper = self.entity.split('_')

            if not extensions['pageInfo']['hasNextPage']:
                break

            data = data.get(entity, [])

            start_from = extensions['pageInfo']['lastId']
            total = extensions['pageInfo']['totalCount']

            # if not wrapper:
            #     data = [dict_to_csvrow(norm(d), self.struct) for d in data]
            # else:
            #     data = transform_nested_gql_response(wrapper, data)
            #     data = [dict_to_csvrow(norm(d), self.struct) for d in data]

            # print(data)

            data = [flatten_data(d) for d in data]
            data = [clean(d) for d in data if len(d.keys()) != 1]
            data = [dict_to_csvrow(d, self.struct) for d in data]

            save_csvrows(self.output().path, data, sep=self.sep, quoter="\"")
            parsed_count += len(data)
            percent = floor((100 * parsed_count) / total)
            s = f'Total: {total}. Parsed: {parsed_count}.'
            self.set_status(s, percent)
            sleep(self.timeout)


class GoszakupCompaniesParsingToCsv(GoszakupGqlParsingToCsv):
    pass


@requires(GoszakupCompaniesParsingToCsv)
class GzipGoszakupCompaniesParsingToCsv(GzipToFtp):
    pass


class GoszakupCompanies(luigi.WrapperTask):

    def requires(self):
        query = """
        query getSubjects($from: String, $to: String, $limit: Int, $after: Int){
          Subjects(filter: {lastUpdateDate: [$from, $to]}, limit: $limit, after: $after) {
            pid
            bin
            iin
            inn
            unp
            regdate
            crdate
            number_reg: numberReg
            series
            name_ru: nameRu
            name_kz: nameKz
            email
            phone
            website
            last_update_date: lastUpdateDate
            country_code: countryCode
            qvazi
            customer
            organizer
            mark_national_company: markNationalCompany
            ref_kopf_code: refKopfCode
            mark_assoc_with_disab: markAssocWithDisab
            year
            mark_resident: markResident
            system_id: systemId
            supplier
            krp_code: krpCode
            oked_list: okedList
            kse_code: kseCode
            mark_world_company: markWorldCompany
            mark_state_monopoly: markStateMonopoly
            mark_natural_monopoly: markNaturalMonopoly
            mark_patronymic_producer: markPatronymicProducer
            mark_patronymic_supplyer: markPatronymicSupplyer
            mark_small_employer: markSmallEmployer
            type_supplier: typeSupplier
            is_single_org: isSingleOrg
            index_date: indexDate
          }
        }
"""
        return GzipGoszakupCompaniesParsingToCsv(
            entity='Subjects',
            anchor_field='pid',
            directory=TMP_DIR,
            ftp_directory='goszakup',
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            query=query,
            name='goszakup_companies',
            struct=GoszakupCompanyRow
        )


class GoszakupContractsParsingToCsv(GoszakupGqlParsingToCsv):
    pass


@requires(GoszakupContractsParsingToCsv)
class GzipGoszakupContractsParsingToCsv(GzipToFtp):
    pass


class GoszakupContracts(luigi.WrapperTask):

    def requires(self):
        query = """
        query getContracts($from: String, $to: String, $limit: Int, $after: Int){
          Contract(filter: {lastUpdateDate: [$from, $to]}, limit: $limit, after: $after) {
            id
            parent_id: parentId
            root_id: rootId
            trd_buy_id: trdBuyId
            trd_buy_number_anno: trdBuyNumberAnno
            trd_buy_name_ru: trdBuyNameRu
            trd_buy_name_kz: trdBuyNameKz
            ref_contract_status_id: refContractStatusId
            deleted
            crdate
            last_update_date: lastUpdateDate
            supplier_id: supplierId
            supplier_biin: supplierBiin
            supplier_bik: supplierBik
            supplier_iik: supplierIik
            supplier_bank_name_kz: supplierBankNameKz
            supplier_bank_name_ru: supplierBankNameRu
            supplier_legal_address: supplierLegalAddress
            supplier_bill_id: supplierBillId
            contract_number: contractNumber
            sign_reason_doc_name: signReasonDocName
            sign_reason_doc_date: signReasonDocDate
            trd_buy_itogi_date_public: trdBuyItogiDatePublic
            customer_id: customerId
            customer_bin: customerBin
            customer_bik: customerBik
            customer_iik: customerIik
            customer_bill_id: customerBillId
            customer_bank_name_kz: customerBankNameKz
            customer_bank_name_ru: customerBankNameRu
            customer_legal_address: customerLegalAddress
            contract_number_sys: contractNumberSys
            payments_terms_ru: paymentsTermsRu
            payments_terms_kz: paymentsTermsKz
            ref_subject_type_id: refSubjectTypeId
            ref_subject_types_id: refSubjectTypesId
            is_gu: isGu
            fin_year: finYear
            ref_contract_agr_form_id: refContractAgrFormId
            ref_contract_year_type_id: refContractYearTypeId
            ref_finsource_id: refFinsourceId
            ref_currency_code: refCurrencyCode
            exchange_rate: exchangeRate
            contract_sum: contractSum
            contract_sum_wnds: contractSumWnds
            sign_date: signDate
            ec_end_date: ecEndDate
            plan_exec_date: planExecDate
            fakt_exec_date: faktExecDate
            fakt_sum: faktSum
            fakt_sum_wnds: faktSumWnds
            contract_end_date: contractEndDate
            ref_contract_cancel_id: refContractCancelId
            ref_contract_type_id: refContractTypeId
            description_kz: descriptionKz
            description_ru: descriptionRu
            fakt_trade_methods_id: faktTradeMethodsId
            ec_customer_approve: ecCustomerApprove
            ec_supplier_approve: ecSupplierApprove
            contract_ms: contractMs
            treasure_req_num: treasureReqNum
            treasure_req_date: treasureReqDate
            treasure_not_num: treasureNotNum
            treasure_not_date: treasureNotDate
            system_id: systemId
            index_date: indexDate
          }
        }
        """
        return GzipGoszakupContractsParsingToCsv(
            entity='Contract',
            directory=TMP_DIR,
            ftp_directory='goszakup',
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            query=query,
            name='goszakup_contracts',
            struct=GoszakupContractRow
        )


class GoszakupLotsParsingToCsv(GoszakupGqlParsingToCsv):
    pass


@requires(GoszakupLotsParsingToCsv)
class GzipGoszakupLotsParsingToCsv(GzipToFtp):
    pass


class GoszakupLots(luigi.WrapperTask):

    def requires(self):
        query = """
        query getLots($from: String, $to: String, $limit: Int, $after: Int){
          Lots(filter: {lastUpdateDate: [$from, $to]}, limit: $limit, after: $after) {
            id
            lot_number: lotNumber
            ref_lot_status_id: refLotStatusId
            last_update_date: lastUpdateDate
            union_lots: unionLots
            point_list: pointList
            count
            amount
            name_ru: nameRu
            name_kz: nameKz
            description_ru: descriptionRu
            description_kz: descriptionKz
            customer_id: customerId
            customer_bin: customerBin
            trd_buy_number_anno: trdBuyNumberAnno
            trd_buy_id: trdBuyId
            dumping
            ref_trade_methods_id: refTradeMethodsId
            psd_sign: psdSign
            consulting_services: consultingServices
            point_list: pointList
            singl_org_sign: singlOrgSign
            is_light_industry: isLightIndustry
            is_construction_work: isConstructionWork
            disable_person_id: disablePersonId
            customer_name_kz: customerNameKz
            customer_name_ru: customerNameRu
            ref_buy_trade_methods_id: refBuyTradeMethodsId
            index_date: indexDate
            system_id: systemId
          }
        }
        """
        return GzipGoszakupLotsParsingToCsv(
            entity='Lots',
            directory=TMP_DIR,
            ftp_directory='goszakup',
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            query=query,
            name='goszakup_lots',
            struct=GoszakupLotsRow
        )


class GoszakupTradeBuyParsingToCsv(GoszakupGqlParsingToCsv):
    pass


@requires(GoszakupTradeBuyParsingToCsv)
class GzipGoszakupTradeBuyParsingToCsv(GzipToFtp):
    pass


class GoszakupLot(luigi.WrapperTask):

    def requires(self):
        query = """
        query getTrdAppLots($from: String, $to: String, $limit: Int, $after: Int){
          TrdAppLots(filter: {lastUpdateDate: [$from, $to]}, limit: $limit, after: $after) {
            
          }
        }
        """
        return GzipGoszakupLotsParsingToCsv(
            entity='Lots',
            directory=TMP_DIR,
            ftp_directory='goszakup',
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            query=query,
            name='goszakup_lots',
            struct=GoszakupLotsRow
        )


class GoszakupTradeBuy(luigi.WrapperTask):

    def requires(self):
        query = """
        query getTradeBuys($from: String, $to: String, $limit: Int, $after: Int){
          TrdBuy(filter: {lastUpdateDate: [$from, $to]}, limit: $limit, after: $after) {
            id
            number_anno: numberAnno
            name_ru: nameRu
            name_kz: nameKz
            total_sum: totalSum
            count_lots: countLots
            ref_trade_methods_id: refTradeMethodsId
            ref_subject_type_id: refSubjectTypeId
            customer_bin: customerBin
            customer_pid: customerPid
            customer_name_kz: customerNameKz
            customer_name_ru: customerNameRu
            org_bin: orgBin
            org_pid: orgPid
            org_name_kz: orgNameKz
            org_name_ru: orgNameRu
            ref_buy_status_id: refBuyStatusId
            start_date: startDate
            repeat_start_date: repeatStartDate
            repeat_end_date: repeatEndDate
            end_date: endDate
            publish_date: publishDate
            itogi_date_public: itogiDatePublic
            ref_type_trade_id: refTypeTradeId
            disable_person_id: disablePersonId
            discus_start_date: discusStartDate
            discus_end_date: discusEndDate
            id_supplier: idSupplier
            biin_supplier: biinSupplier
            parent_id: parentId
            singl_org_sign: singlOrgSign
            is_light_industry: isLightIndustry
            is_construction_work: isConstructionWork
            system_id: systemId
            index_date: indexDate
          }
        }
        """
        return GzipGoszakupTradeBuyParsingToCsv(
            entity='TrdBuy',
            directory=TMP_DIR,
            ftp_directory='goszakup',
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            query=query,
            name='goszakup_trdbuy',
            struct=GoszakupTradeBuyRow
        )


class GoszakupPlanPointsParsingToCsv(GoszakupGqlParsingToCsv):
    pass


@requires(GoszakupPlanPointsParsingToCsv)
class GzipGoszakupPlanPointsParsingToCsv(GzipToFtp):
    pass


class GoszakupPlanPoints(luigi.WrapperTask):

    def requires(self):
        query = """
        query getPlanPoints($from: String, $to: String, $limit: Int, $after: Int){
          Plans(filter: {timestamp: [$from, $to]}, limit: $limit, after: $after) {
            id
            rootrecord_id: rootrecordId
            sys_subjects_id: sysSubjectsId
            sys_organizator_id: sysOrganizatorId
            subject_biin: subjectBiin
            subject_name_ru: subjectNameRu
            subject_name_kz: subjectNameKz
            name_ru: nameRu
            name_kz: nameKz
            ref_trade_methods_id: refTradeMethodsId
            ref_units_code: refUnitsCode
            count
            price
            amount
            ref_months_id: refMonthsId
            ref_pln_point_status_id: refPlnPointStatusId
            pln_point_year: plnPointYear
            ref_subject_type_id: refSubjectTypeId
            ref_enstru_code: refEnstruCode
            ref_finsource_id: refFinsourceId
            ref_abp_code: refAbpCode
            is_qvazi: isQvazi
            date_create: dateCreate
            timestamp: timestamp
            ref_point_type_id: refPointTypeId
            desc_ru: descRu
            desc_kz: descKz
            extra_desc_kz: extraDescKz
            extra_desc_ru: extraDescRu
            sum_1: sum1
            sum_2: sum2
            sum_3: sum3
            supply_date_ru: supplyDateRu
            prepayment: prepayment
            ref_justification_id: refJustificationId
            ref_amendment_agreem_type_id: refAmendmentAgreemTypeId
            ref_amendm_agreem_justif_id: refAmendmAgreemJustifId
            contract_prev_point_id: contractPrevPointId
            disable_person_id: disablePersonId
            transfer_sys_subjects_id: transferSysSubjectsId
            transfer_type: transferType
            ref_budget_type_id: refBudgetTypeId
            createdin_act_id: createdinActId
            is_active: isActive
            active_act_id: activeActId
            is_deleted: isDeleted
            system_id: systemId
            index_date: indexDate
            _: PlanActs{
                plan_act_id: id
                number: planActNumber
                ref_plan_status_id: refPlanStatusId
                plan_fin_year: planFinYear
                plan_preliminary: planPreliminary
                date_approved: dateApproved
            }
          }
        }
        """
        return GzipGoszakupPlanPointsParsingToCsv(
            entity='Plans',
            directory=TMP_DIR,
            ftp_directory='goszakup',
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            query=query,
            name='goszakup_plan_point',
            struct=GoszakupPlanPointRow
        )


class GoszakupPlanKatoParsingToCsv(GoszakupGqlParsingToCsv):
    pass


@requires(GoszakupPlanKatoParsingToCsv)
class GzipGoszakupPlanKatoParsingToCsv(GzipToFtp):
    pass


class GoszakupPlanKato(luigi.WrapperTask):

    def requires(self):
        query = """
        query getPlansKato($from: String, $to: String, $limit: Int, $after: Int){
                  Plans(filter: {timestamp: [$from, $to]}, limit: $limit, after: $after) {
                    _: PlansKato {
                        id
                        pln_points_id: plnPointsId
                        ref_kato_code: refKatoCode
                        ref_countries_code: refCountriesCode
                        full_delivery_place_name_ru: fullDeliveryPlaceNameRu
                        full_delivery_place_name_kz: fullDeliveryPlaceNameKz
                        count
                        is_active: isActive
                        is_deleted: isDeleted
                        system_id: systemId
                        index_date: indexDate
                    }
                }
            }
        """
        return GzipGoszakupPlanKatoParsingToCsv(
            entity='Plans_Kato',
            directory=TMP_DIR,
            ftp_directory='goszakup',
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            query=query,
            name='goszakup_plan_kato',
            struct=GoszakupPlanKatoRow,
            anchor_field='pln_points_id'
        )


class GoszakupContractUnitsParsingToCsv(GoszakupGqlParsingToCsv):
    pass


@requires(GoszakupContractUnitsParsingToCsv)
class GzipGoszakupContractUnitsParsingToCsv(GzipToFtp):
    pass


class GoszakupContractUnits(luigi.WrapperTask):

    def requires(self):
        query = """
            query getContractUnits($from: String, $to: String, $limit: Int, $after: Int){
                  Contract(filter: {lastUpdateDate: [$from, $to]}, limit: $limit, after: $after) {
                    contract_id: id
                    _: ContractUnits{
                        id
                        lot_id: lotId
                        pln_point_id: plnPointId
                        item_price: itemPrice
                        item_price_wnds: itemPriceWnds
                        quantity
                        total_sum: totalSum
                        total_sum_wnds: totalSumWnds
                        fact_sum: factSum
                        fact_sum_wnds: factSumWnds
                        ks_proc: ksProc
                        ks_sum: ksSum
                        deleted
                        trd_buy_id: trdBuyId
                        contract_registry_id: contractRegistryId
                        crdate
                        exec_fakt_date: execFaktDate
                        exec_plan_date: execPlanDate
                        executed
                        parent_id: parentId
                        root_id: rootId
                        ref_contract_status_id: refContractStatusId
                        cr_deleted: crDeleted
                        ref_amendm_agreem_justif_id: refAmendmAgreemJustifId
                        system_id: systemId
                        index_date: indexDate
                    }
                    
                }
            }
        """
        return GzipGoszakupContractUnitsParsingToCsv(
            entity='Contract_Units',
            directory=TMP_DIR,
            ftp_directory='goszakup',
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            query=query,
            name='goszakup_contract_units',
            struct=GoszakupContractUnitsRow,
            anchor_field='contract_id'
        )


class GoszakupTrdAppOffersParsingToCsv(GoszakupGqlParsingToCsv):
    pass


@requires(GoszakupTrdAppOffersParsingToCsv)
class GzipGoszakupTrdAppOffersParsingToCsv(GzipToFtp):
    pass


class GoszakupTrdAppOffers(luigi.WrapperTask):

    def requires(self):
        query = """
            query getTrd($from: String, $to: String, $limit: Int, $after: Int){
                TrdApp(filter: {dateApply: [$from, $to]}, limit: $limit, after: $after){
                    _ : AppLots {
                        _: Offers {
                            id: id
                            lot_id: lotId
                            app_lot_id: appLotId
                            price: price
                            amount: amount
                            system_id: systemId
                            index_date: indexDate
                        }
                        app_lot_point_list: pointList
                        app_lot_status_id: statusId
                        app_lot_price: price
                        app_lot_amount: amount
                        app_lot_discount_value: discountValue
                        app_lot_discount_price: discountPrice
                        app_lot_system_id: systemId
                        app_lot_index_date: indexDate
                    }
                    app_id: id
                    app_buy_id: buyId
                    app_supplier_id: supplierId
                    app_cr_fio: crFio
                    app_mod_fio: modFio
                    app_supplier_bin_iin: supplierBinIin
                    app_prot_id: protId
                    app_prot_number: protNumber
                    app_date_apply: dateApply
                    app_system_id: systemId
                    app_index_date: indexDate
                }
            }
        """
        return GzipGoszakupTrdAppOffersParsingToCsv(
            entity='TrdApp',
            directory=BIGDATA_TMP_DIR,
            ftp_directory='goszakup',
            start_date='2022-03-01 00:00:00.000000',
            end_date='2022-05-31 23:59:59.000000',
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            query=query,
            name='goszakup_trd_app_offers',
            struct=GoszakupTrdAppOffersRow,
            anchor_field='app_id',
            timeout=2
        )


if __name__ == '__main__':
    luigi.run()
