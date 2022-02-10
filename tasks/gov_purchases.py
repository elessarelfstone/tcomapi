import json
import os
from urllib.parse import urlparse


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
                                  append_file, get_file_lines_count)


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


def get_total(url: str, headers: str):
    r = get(url, headers=headers)
    return Box(json.loads(r)).total


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
        parsed_count = get_file_lines_count(self.output().path)
        parsed_count = 0 if not parsed_count  else parsed_count

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




class GoszakupGqlParsingToCsv(GraphQlParsing):
    entity = luigi.Parameter()
    start_date = luigi.Parameter(default=previous_date_as_str(1))
    # start_date = luigi.Parameter(default='2021-09-23')
    # end_date = luigi.Parameter(default=previous_date_as_str(1))
    end_date = luigi.Parameter(default=previous_date_as_str(1))
    limit = luigi.IntParameter(default=200)
    anchor_field = luigi.Parameter(default='id')

    def run(self):
        client = self.get_client()
        query = gql(self.query)
        start_from = None
        params = {'from': str(self.start_date), 'to': str(self.end_date), 'limit': self.limit}

        super().run()
        while True:
            p = params
            if start_from:
                p["after"] = start_from

            data = client.execute(query, variable_values=p)
            if data.get(self.entity) is None or len(data.get(self.entity, [])) == 0:
                break

            last_id = data.get(self.entity, [])[-1][self.anchor_field]
            start_from = last_id
            data = [dict_to_csvrow(d, self.struct) for d in data.get(self.entity)]
            save_csvrows(self.output().path, data, sep=self.sep, quoter="\"")


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


if __name__ == '__main__':
    luigi.run()
