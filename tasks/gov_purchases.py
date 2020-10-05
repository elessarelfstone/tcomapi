import json
import os
from urllib.parse import urlparse


import attr
import luigi
from box import Box
from gql import gql
from gql.transport.exceptions import TransportServerError
from luigi.util import requires
from time import sleep

from settings import TMP_DIR, BIGDATA_TMP_DIR
from tasks.base import GzipToFtp, LoadingDataIntoCsvFile, BigDataToCsv
from tasks.grql import GraphQlParsing, GraphQlBigDataParsing
from tcomapi.common.dates import previous_date_as_str
from tcomapi.common.utils import (dict_to_csvrow, save_csvrows, get,
                                  get_lastrow_ncolumn_value_in_csv, read_lines,
                                  append_file, get_file_lines_count)


@attr.s
class GovernmentPurchasesCompanyRow:
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
class GovernmentPurchasesContractRow:
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


def get_total(url: str, headers: str):
    r = get(url, headers=headers)
    return Box(json.loads(r)).total


class GoszakupAllRowsParsing(BigDataToCsv, LoadingDataIntoCsvFile):

    url = luigi.Parameter()
    token = luigi.Parameter()
    # headers = luigi.DictParameter(default={})
    timeout = luigi.IntParameter(default=10)
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
        append_file(self.success_fpath, stat)


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
                                                    token='Bearer 61b536c8271157ab23f71c745b925133',
                                                    name='goszakup_contracts',
                                                    struct=GovernmentPurchasesContractRow)


class GovernmentPurchasesCompaniesParsingToCsv(GraphQlParsing):
    start_date = luigi.Parameter(default=previous_date_as_str(1))
    end_date = luigi.Parameter(default=previous_date_as_str(1))
    limit = 100

    def run(self):
        client = self.get_client()
        query = gql(self.query)
        start_from = None
        params = {'from': str(self.start_date), 'to': str(self.end_date), 'limit': self.limit}

        header = tuple(f.name for f in attr.fields(GovernmentPurchasesCompanyRow))
        save_csvrows(self.output().path, [header], sep=self.sep)

        while True:
            p = params
            if start_from:
                p["after"] = start_from

            data = client.execute(query, variable_values=p)
            if data.get('Subjects') is None or len(data.get('Subjects', [])) == 0:
                break

            last_id = data.get('Subjects', [])[-1]['pid']
            start_from = last_id
            data = [dict_to_csvrow(d, self.struct) for d in data.get('Subjects')]
            save_csvrows(self.output().path, data, sep=self.sep, quoter="\"")


@requires(GovernmentPurchasesCompaniesParsingToCsv)
class GzipGovernmentPurchasesCompaniesParsingToCsv(GzipToFtp):
    pass


class GovernmentPurchasesCompanies(luigi.WrapperTask):

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
        return GzipGovernmentPurchasesCompaniesParsingToCsv(
            directory=TMP_DIR,
            sep=',',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            headers={'Authorization': 'Bearer 61b536c8271157ab23f71c745b925133'},
            query=query,
            name='goszakup_companies',
            struct=GovernmentPurchasesCompanyRow
        )


class GovernmentPurchasesContractsParsingToCsv(GraphQlParsing):

    start_date = luigi.Parameter(default=previous_date_as_str(1))
    end_date = luigi.Parameter(default=previous_date_as_str(1))

    def run(self):
        client = self.get_client()
        query = gql(self.query)
        start_from = None
        params = {'from': str(self.start_date), 'to': str(self.end_date), 'limit': self.limit}

        while True:
            p = params
            if start_from:
                p["after"] = start_from

            data = client.execute(query, variable_values=p)
            if data.get('Contract') is None or len(data.get('Contract', [])) == 0:
                break

            last_id = data.get('Contract', [])[-1]['id']
            start_from = last_id
            data = [dict_to_csvrow(d, self.struct) for d in data.get('Contract')]
            save_csvrows(self.output().path, data, sep=self.sep, quoter="\"")


@requires(GovernmentPurchasesContractsParsingToCsv)
class GzipGovernmentPurchasesContractsParsingToCsv(GzipToFtp):
    pass


class GovernmentPurchasesContracts(luigi.WrapperTask):

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
        return GzipGovernmentPurchasesContractsParsingToCsv(
            directory=TMP_DIR,
            sep=';',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            headers={'Authorization': 'Bearer 61b536c8271157ab23f71c745b925133'},
            query=query,
            name='goszakup_contracts',
            struct=GovernmentPurchasesContractRow
        )


if __name__ == '__main__':
    luigi.run()
