
import attr
import luigi
from box import Box
from gql import gql
from luigi.util import requires

from settings import TMP_DIR
from tasks.base import GzipToFtp
from tasks.grql import GraphQlParsing
from tcomapi.common.dates import previous_date_as_str
from tcomapi.common.utils import dict_to_csvrow, save_csvrows


@attr.s
class GovernmentPurchasesRow:
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


class GovernmentPurchasesParsingToCsv(GraphQlParsing):
    start_date = luigi.Parameter(default=previous_date_as_str(1))
    end_date = luigi.Parameter(default=previous_date_as_str(1))
    limit = 100

    def run(self):
        client = self.get_client()
        query = gql(self.query)
        start_from = None
        params = {'from': str(self.start_date), 'to': str(self.end_date), 'limit': self.limit}

        header = tuple(f.name for f in attr.fields(GovernmentPurchasesRow))
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


@requires(GovernmentPurchasesParsingToCsv)
class GzipGovernmentPurchasesParsingToCsv(GzipToFtp):
    pass


class GovernmentPurchases(luigi.WrapperTask):

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
        return GzipGovernmentPurchasesParsingToCsv(
            directory=TMP_DIR,
            sep=',',
            url='https://ows.goszakup.gov.kz/v3/graphql',
            headers={'Authorization': 'Bearer 61b536c8271157ab23f71c745b925133'},
            query=query,
            name='goszakup_companies',
            struct=GovernmentPurchasesRow
        )


if __name__ == '__main__':
    luigi.run()
