import datetime
import os
import re
from math import floor


import attr
import luigi
from box import Box

from luigi.util import requires
from luigi.parameter import ParameterVisibility

from settings import (TMP_DIR, BIGDATA_TMP_DIR, SAMRUK_API_HOST, SAMRUK_API_USER, SAMRUK_API_PASSWORD,
                      SAMRUK_API_TIMEOUT, SAMRUK_API_TCOM_ID)
from tasks.base import GzipToFtp, BigDataToCsv, ExternalLocalTarget, BaseRunner

from tcomapi.samruk.parser import SamrukApiParser, SamrukPlansApiParser
from tcomapi.common.correctors import basic_corrector
from tcomapi.common.dates import today_as_str, DEFAULT_DATE_FORMAT, LastPeriod
from tcomapi.common.utils import (dict_to_csvrow, save_csvrows,
                                  append_file, read_lines, build_fpath)

SK_BASE_URL = 'https://integr.skc.kz/'


def default_corrector(value):
    if value is None:
        return ''

    return value


def cert_corrector(value):
    if value is None:
        return ''

    v = basic_corrector(value)
    return value.replace('\r', '').replace('\n', '')


def cert_corrector2(value):
    if value is None:
        return ''

    return value.replace('\r', '').replace('\n', '')


@attr.s
class SamrukSupplierRow:
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
class SamrukPurchaseRow:
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
class SamrukContractRow:
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
class SamrukBadSupplierRow:
    bin_iin = attr.ib(converter=default_corrector, default='')
    name_ru = attr.ib(converter=default_corrector, default='')
    full_name_ru = attr.ib(converter=default_corrector, default='')
    reason_ru = attr.ib(converter=default_corrector, default='')
    created_date = attr.ib(converter=default_corrector, default='')
    start_date = attr.ib(converter=default_corrector, default='')
    end_date = attr.ib(converter=default_corrector, default='')


@attr.s
class SamrukKztPlanRow:
    id = attr.ib(converter=default_corrector, default='')
    year = attr.ib(converter=default_corrector, default='')
    type = attr.ib(converter=default_corrector, default='')
    durationtype = attr.ib(converter=default_corrector, default='')
    itemcount = attr.ib(converter=default_corrector, default='')
    approvedplansum = attr.ib(converter=default_corrector, default='')


@attr.s
class SkKztContractSubjectsRow:
    id = attr.ib(converter=default_corrector, default='')
    count = attr.ib(converter=default_corrector, default='')
    execution_sum_nds = attr.ib(converter=default_corrector, default='')
    execution_sum_no_nds = attr.ib(converter=default_corrector, default='')
    foreign_price = attr.ib(converter=default_corrector, default='')
    foreign_sum = attr.ib(converter=default_corrector, default='')
    sum_nds = attr.ib(converter=default_corrector, default='')
    sum_no_nds = attr.ib(converter=default_corrector, default='')
    contract_card_id = attr.ib(converter=default_corrector, default='')
    lot_id = attr.ib(converter=default_corrector, default='')
    plan_item_id = attr.ib(converter=default_corrector, default='')
    price = attr.ib(converter=default_corrector, default='')
    country_id = attr.ib(converter=default_corrector, default='')
    prev_contract_item_id = attr.ib(converter=default_corrector, default='')
    delivery_date_time = attr.ib(converter=default_corrector, default='')
    final_payment = attr.ib(converter=default_corrector, default='')
    interim_payment = attr.ib(converter=default_corrector, default='')
    prepayment = attr.ib(converter=default_corrector, default='')
    detail_ru = attr.ib(converter=default_corrector, default='')
    nds_size_id = attr.ib(converter=default_corrector, default='')
    local_content = attr.ib(converter=default_corrector, default='')
    execution_count = attr.ib(converter=default_corrector, default='')
    stkz_certificate_position_id = attr.ib(converter=default_corrector, default='')
    local_content_projected_share = attr.ib(converter=default_corrector, default='')
    foreign_execution_sum = attr.ib(converter=default_corrector, default='')
    jhi_comment = attr.ib(converter=default_corrector, default='')
    status = attr.ib(converter=default_corrector, default='')
    year_count = attr.ib(converter=default_corrector, default='')
    foreign_price_year = attr.ib(converter=default_corrector, default='')
    foreign_sum_year = attr.ib(converter=default_corrector, default='')
    price_year = attr.ib(converter=default_corrector, default='')
    jhi_year = attr.ib(converter=default_corrector, default='')
    sum_nds_year = attr.ib(converter=default_corrector, default='')
    sum_no_nds_year = attr.ib(converter=default_corrector, default='')


@attr.s
class SamrukKztPlanItemRow:
    id = attr.ib(converter=default_corrector, default='')
    planid = attr.ib(converter=default_corrector, default='')
    tenderpriority = attr.ib(converter=default_corrector, default='')
    rownumber = attr.ib(converter=default_corrector, default='')
    extid = attr.ib(converter=default_corrector, default='')
    tendersubjecttype = attr.ib(converter=default_corrector, default='')
    mkeicode = attr.ib(converter=default_corrector, default='')
    price = attr.ib(converter=default_corrector, default='')
    priceconfidenceto = attr.ib(converter=default_corrector, default='')
    priceaverage = attr.ib(converter=default_corrector, default='')
    prevpriceconfidenceto = attr.ib(converter=default_corrector, default='')
    prevpriceaverage = attr.ib(converter=default_corrector, default='')
    priceexceeded = attr.ib(converter=default_corrector, default='')
    count = attr.ib(converter=default_corrector, default='')
    sumtrunonds = attr.ib(converter=default_corrector, default='')
    sumtrunds = attr.ib(converter=default_corrector, default='')
    tendertype = attr.ib(converter=default_corrector, default='')
    localcontent = attr.ib(converter=default_corrector, default='')
    katocode = attr.ib(converter=default_corrector, default='')
    tenderlocation = attr.ib(converter=default_corrector, default='')
    durationmonth = attr.ib(converter=default_corrector, default='')
    incotermsconditioncode = attr.ib(converter=default_corrector, default='')
    deliverycountrycode = attr.ib(converter=default_corrector, default='')
    deliverylocation = attr.ib(converter=default_corrector, default='')
    deliverykatocode = attr.ib(converter=default_corrector, default='')
    deleted = attr.ib(converter=default_corrector, default='')
    hidden = attr.ib(converter=default_corrector, default='')
    nonds = attr.ib(converter=default_corrector, default='')
    transienttender = attr.ib(converter=default_corrector, default='')
    trucode = attr.ib(converter=default_corrector, default='')
    organizerbin = attr.ib(converter=default_corrector, default='')
    subdivision = attr.ib(converter=default_corrector, default='')
    sort = attr.ib(converter=default_corrector, default='')
    addattributeru = attr.ib(converter=default_corrector, default='')
    addattributekk = attr.ib(converter=default_corrector, default='')
    singlesourcereason = attr.ib(converter=default_corrector, default='')
    createddate = attr.ib(converter=default_corrector, default='')
    createduser = attr.ib(converter=default_corrector, default='')
    lastmodifieddate = attr.ib(converter=default_corrector, default='')
    lastmodifieduser = attr.ib(converter=default_corrector, default='')
    executioncount = attr.ib(converter=default_corrector, default='')
    executionsumnonds = attr.ib(converter=default_corrector, default='')
    prevcontractcardstatushistory = attr.ib(converter=default_corrector, default='')
    contractcardtype = attr.ib(converter=default_corrector, default='')
    checkedskb = attr.ib(converter=default_corrector, default='')


@attr.s
class SamrukCertRow:
    id = attr.ib(default='')
    bin = attr.ib(converter=cert_corrector, default='')
    certificate_id = attr.ib(default='')
    description_kk = attr.ib(converter=cert_corrector, default='')
    description_ru = attr.ib(converter=cert_corrector, default='')
    director_name_kk = attr.ib(converter=cert_corrector, default='')
    director_name_ru = attr.ib(converter=cert_corrector, default='')
    expiration_date = attr.ib(converter=cert_corrector, default='')
    issue_date = attr.ib(converter=cert_corrector, default='')
    kato_code = attr.ib(converter=cert_corrector, default='')
    modified_date = attr.ib(converter=cert_corrector, default='')
    name_kk = attr.ib(converter=cert_corrector, default='')
    name_ru = attr.ib(converter=cert_corrector, default='')
    jhi_number = attr.ib(converter=cert_corrector, default='')
    organization_code = attr.ib(converter=cert_corrector, default='')
    series = attr.ib(converter=cert_corrector, default='')
    id_stkz_certificate_position = attr.ib(default='')
    box_type = attr.ib(converter=cert_corrector, default='')
    count = attr.ib(default='')
    percent = attr.ib(default='')
    tnved = attr.ib(converter=cert_corrector, default='')
    unit_code = attr.ib(converter=cert_corrector, default='')
    description_kk_stkz_certificate_position = attr.ib(converter=cert_corrector, default='')
    description_ru_stkz_certificate_position = attr.ib(converter=cert_corrector, default='')


@attr.s
class SamrukDictRow:
    id = attr.ib(default='')
    created_date = attr.ib(converter=basic_corrector, default='')
    code = attr.ib(converter=basic_corrector, default='')
    en = attr.ib(converter=basic_corrector, default='')
    ru = attr.ib(converter=basic_corrector, default='')
    kk = attr.ib(converter=basic_corrector, default='')
    version = attr.ib(default='')


class SamrukBaseRunner(BaseRunner):
    @property
    def get_after(self):
        p = LastPeriod.get_period(self.last_period)
        if p:
            return p[0]
        return p


class SamrukParsing(BigDataToCsv):

    uri = luigi.Parameter()

    entity = luigi.Parameter(default='content')

    limit = luigi.IntParameter(default=100)
    after = luigi.DateParameter(default=None)

    user = luigi.Parameter(default=SAMRUK_API_USER,
                           visibility=ParameterVisibility.HIDDEN)
    password = luigi.Parameter(default=SAMRUK_API_PASSWORD,
                               visibility=ParameterVisibility.HIDDEN)

    timeout = luigi.IntParameter(default=int(SAMRUK_API_TIMEOUT))

    @property
    def url(self):
        return f'{SAMRUK_API_HOST}{self.uri}'

    @property
    def params(self):

        # initializing parameters
        params = Box()
        params.page = 0
        params.size = self.limit

        # samruk api doesn't have full support
        # for retrieving data for some period
        # it's only after parameter
        if self.after:
            params.after = str(self.after)

        return params

    def run(self):
        params = self.params
        parser = SamrukApiParser(self.url, self.entity,
                                 self.timeout, self.user, self.password,
                                 params.to_dict())

        for rows in parser:
            data = [dict_to_csvrow(d, self.struct) for d in rows]
            save_csvrows(self.output().path, data)
            self.set_status(parser.status_message, parser.percent_done)

        append_file(self.success_fpath, 'good')


class SamrukSuppliersParsingToCsv(SamrukParsing):
    pass


@requires(SamrukSuppliersParsingToCsv)
class SamrukSuppliersUpload(GzipToFtp):
    pass


class SamrukSuppliers(SamrukBaseRunner):
    def requires(self):

        return SamrukSuppliersUpload(
            directory=TMP_DIR,
            ftp_directory='samruk',
            sep=';',
            uri='data/suppliers/supplierList',
            name='samruk_suppliers',
            struct=SamrukSupplierRow,
            after=self.get_after
        )


class SamrukBadSuppliersParsingToCsv(SamrukParsing):
    pass


@requires(SamrukBadSuppliersParsingToCsv)
class SamrukBadSuppliersUpload(GzipToFtp):
    pass


class SamrukBadSuppliers(SamrukBaseRunner):
    def requires(self):

        return SamrukBadSuppliersUpload(
            directory=TMP_DIR,
            ftp_directory='samruk',
            sep=';',
            uri='data/bad-supplier/badSupplierList',
            name='samruk_bad_suppliers',
            struct=SamrukBadSupplierRow,
            after=self.get_after
        )


class SamrukKztPurchasesParsingToCsv(SamrukParsing):

    @property
    def params(self):
        params = super().params
        params.login = self.user
        return params


@requires(SamrukKztPurchasesParsingToCsv)
class SamrukKztPurchasesUpload(GzipToFtp):
    pass


class SamrukKztPurchases(SamrukBaseRunner):
    def requires(self):
        return SamrukKztPurchasesUpload(
            directory=TMP_DIR,
            ftp_directory='samruk',
            sep=';',
            uri='data/purchases/purchaseList',
            name='samruk_kzt_purchases',
            struct=SamrukPurchaseRow,
            after=self.get_after
        )


class SamrukKztContractsParsingToCsv(SamrukParsing):

    company_id = luigi.Parameter(visibility=ParameterVisibility.HIDDEN)

    @property
    def params(self):
        params = super().params
        params.login = self.user
        params.companyIdentifier = self.company_id
        return params


@requires(SamrukKztContractsParsingToCsv)
class SamrukKztContractsUpload(GzipToFtp):
    pass


class SamrukKztContracts(SamrukBaseRunner):
    def requires(self):
        return SamrukKztContractsUpload(
            directory=TMP_DIR,
            ftp_directory='samruk',
            sep=';',
            uri='data/contract/contractList',
            name='samruk_kzt_contracts',
            struct=SamrukContractRow,
            company_id=SAMRUK_API_TCOM_ID,
            after=self.get_after
        )


class SamrukKztContractSubjectsParsingToCsv(SamrukParsing):

    company_id = luigi.Parameter(visibility=ParameterVisibility.HIDDEN)

    @property
    def params(self):
        params = super().params
        params.login = self.user
        params.companyIdentifier = self.company_id
        return params


@requires(SamrukKztContractSubjectsParsingToCsv)
class SamrukKztContractSubjectsUpload(GzipToFtp):
    pass


class SamrukKztContractSubjects(SamrukBaseRunner):
    def requires(self):
        return SamrukKztContractSubjectsUpload(
            directory=TMP_DIR,
            ftp_directory='samruk',
            sep=';',
            uri='data/contract/contractSubjectList',
            name='samruk_kzt_contract_subjects',
            struct=SkKztContractSubjectsRow,
            company_id=SAMRUK_API_TCOM_ID,
            after=self.get_after
        )


class SamrukPlansParsing(SamrukParsing):
    def run(self):
        params = self.params
        parser = SamrukPlansApiParser(self.url, self.entity,
                                      self.timeout, self.user, self.password,
                                      params.to_dict())

        for rows in parser:
            data = [dict_to_csvrow(d, self.struct) for d in rows]
            save_csvrows(self.output().path, data)
            self.set_status(parser.status_message, parser.percent_done)

        append_file(self.success_fpath, 'good')


class SamrukKztPlansParsingToCsv(SamrukPlansParsing):
    pass


@requires(SamrukKztPlansParsingToCsv)
class SamrukKztPlansUpload(GzipToFtp):
    pass


class SamrukKztPlans(luigi.WrapperTask):
    def requires(self):
        return SamrukKztPlansUpload(
            directory=TMP_DIR,
            ftp_directory='samruk',
            sep=';',
            uri='proxy/planproxy/esb-api/plan',
            name='samruk_kzt_plans',
            struct=SamrukKztPlanRow
        )


class SamrukPlanItemsParsing(SamrukParsing):

    # buffers
    plan_id = None
    failed_page = None
    total_parsed_count = 0

    def requires(self):
        return ExternalLocalTarget('samruk_kzt_plans')

    def lock_data(self):

        data = Box()

        if os.path.exists(self.lock_fpath):
            # raw = read_file(self.lock_fpath)
            # raw = raw.replace("'", '"')
            # data = json.loads(raw)
            data = Box().from_json(filename=self.lock_fpath)

        return data

    @property
    def params(self):
        params = super().params
        params.planId = self.plan_id

        if self.failed_page:
            params.page = self.failed_page

        return params

    def plans_ids(self):

        rows = read_lines(self.input().path)
        plan_ids = [row.split(self.sep)[0] for row in rows]

        # get meta data
        d = self.lock_data()

        last_id = None

        if d:
            last_id = d.last_id

        return plan_ids

    def status(self):
        # TODO implement override it with additional info about curr conv id and count of left ids ot parse
        return f'Current conversation ID:{self.plan_id}. Total parsed count: {self.total_parsed_count}'

    def run(self):

        plans_ids = self.plans_ids()

        meta = self.lock_data()

        # page for last successfully parsed conversation
        if meta:
            self.failed_page = meta.page + 1

        parsed_plans_count = 0
        parsed_total_items = 0
        plans_total = len(plans_ids)

        for p_id in plans_ids:
            self.plan_id = p_id
            params = self.params
            parser = SamrukPlansApiParser(self.url, self.entity, self.timeout,
                                          self.user, self.password, params.to_dict())

            for rows in parser:
                _rows = []
                for d in rows:
                    _rows.append({**d, **{'planid': p_id}})

                data = [dict_to_csvrow(d, self.struct) for d in _rows]
                save_csvrows(self.output().path, data)
                meta.page, meta.last_id = parser.page, p_id
                self.lock(meta.to_json())

                parsed_total_items += len(rows)
                status = f'Total conversation IDs: {len(plans_ids)}.'
                status += f' Parsed plan IDs: {parsed_plans_count} ' + '\n'
                status += f'Current conversation ID: {p_id}. Total parsed items: {parsed_total_items}. '
                status = status + '\n' + parser.status_message
                percentage = floor((parsed_plans_count * 100) / plans_total)
                self.set_status(status, percentage)

            parsed_plans_count += 1

        self.success('good')


class SamrukKztPlanItemsParsingToCsv(SamrukPlanItemsParsing):
    pass


@requires(SamrukKztPlanItemsParsingToCsv)
class SamrukKztPlanItemsUpload(GzipToFtp):
    pass


class SamrukKztPlanItems(luigi.WrapperTask):
    def requires(self):
        return SamrukKztPlanItemsUpload(
            directory=TMP_DIR,
            ftp_directory='samruk',
            sep=';',
            uri='proxy/planproxy/esb-api/plan-item',
            name='samruk_kzt_plan_items',
            struct=SamrukKztPlanItemRow
        )


class SamrukCertsParsingToCsv(SamrukParsing):
    pass


@requires(SamrukCertsParsingToCsv)
class SamrukCertsUpload(GzipToFtp):
    pass


class SamrukCerts(SamrukBaseRunner):
    def requires(self):
        return SamrukCertsUpload(
            directory=BIGDATA_TMP_DIR,
            ftp_directory='samruk',
            sep=';',
            uri='data/stkz-certificate/stkzCertificateList',
            name='samruk_certs',
            struct=SamrukCertRow,
            after=self.get_after
        )


class SamrukDictsParsingToCsv(SamrukParsing):
    pass


@requires(SamrukDictsParsingToCsv)
class SamrukDictsUpload(GzipToFtp):
    pass


class SamrukDicts(SamrukBaseRunner):
    def requires(self):
        return SamrukDictsUpload(
            directory=TMP_DIR,
            ftp_directory='samruk',
            sep=';',
            uri='data/dictionary/dictionaryList',
            name='samruk_dicts',
            struct=SamrukDictRow
        )


if __name__ == '__main__':
    luigi.run()
