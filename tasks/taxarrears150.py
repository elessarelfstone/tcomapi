import attr
import luigi
from luigi.util import requires

from settings import TMP_DIR
from tasks.base import GzipToFtp, WebExcelFileParsingToCsv


@attr.s
class Row:
    num = attr.ib(default='')
    region = attr.ib(default='')
    office_of_tax_enforcement = attr.ib(default='')
    ote_id = attr.ib(default='')
    bin = attr.ib(default='')
    rnn = attr.ib(default='')
    taxpayer_organization_ru = attr.ib(default='')
    taxpayer_organization_kz = attr.ib(default='')
    last_name_kz = attr.ib(default='')
    first_name_kz = attr.ib(default='')
    middle_name_kz = attr.ib(default='')
    last_name_ru = attr.ib(default='')
    first_name_ru = attr.ib(default='')
    middle_name_ru = attr.ib(default='')
    owner_iin = attr.ib(default='')
    owner_rnn = attr.ib(default='')
    owner_name_kz = attr.ib(default='')
    owner_name_ru = attr.ib(default='')
    economic_sector = attr.ib(default='')
    total_due = attr.ib(default='')
    sub_total_main = attr.ib(default='')
    sub_total_late_fee = attr.ib(default='')
    sub_total_fine = attr.ib(default='')


url = 'http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/TAX_ARREARS_150/KZ_ALL/fileName/list_TAX_ARREARS_150_KZ_ALL.xlsx'


@requires(WebExcelFileParsingToCsv)
class GzipDebtorsToFtp(GzipToFtp):
    pass


class TaxArrears150(luigi.WrapperTask):
    def requires(self):
        return GzipDebtorsToFtp(url=url, name='kgd_taxarrears150',
                                monthly=True,
                                struct=Row, sheets=(0, 1),
                                directory=TMP_DIR, skiptop=6)


if __name__ == '__main__':
    luigi.run()
