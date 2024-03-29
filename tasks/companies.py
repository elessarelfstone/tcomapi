import re
import os
import urllib3
from os.path import basename
# from urllib.parse import urlparse
from urllib.parse import urlsplit
from shutil import move

import attr
import luigi
import requests
from bs4 import BeautifulSoup
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tcomapi.common.excel import parse, parse_excel_rect_area_to_csv
from tcomapi.common.utils import (save_csvrows, save_webfile,
                                  build_fpath, append_file, read_lines)

from tcomapi.common.unpacking import unpack, zipo_flist, unzip_one_file
from tcomapi.sgov.api import SgovRCutParser
from settings import CONFIG_DIR, TMP_DIR
from tasks.base import GzipToFtp, BaseConfig

config_path = os.path.join(CONFIG_DIR, 'companies.conf')
add_config_path(config_path)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

rcut_legal_entities = 'legal_entities'
rcut_legal_branches = 'legal_branches'
rcut_joint_ventures = 'joint_ventures'
rcut_foreign_branches = 'foreign_branches'
rcut_entrepreneurs = 'entrepreneurs'


rcut_entr = [
    (77208141, 'abai'),
    (247783, 'akmola'),
    (248875, 'aktobe'),
    (250502, 'almatyobl'),
    (252311, 'atyrau'),
    (253160, 'oral'),
    (255577, 'taraz'),
    (77208139, 'jetisy'),
    (256619, 'karaganda'),
    (258742, 'kostanai'),
    (261475, 'ygobl'),
    (260099, 'kzilorda'),
    (260907, 'aktau'),
    (263009, 'pavlodar'),
    (264023, 'petropavl'),
    (20243032, 'turkestan'),
    (77208140, 'ulitau'),
    (264990, 'oskemen'),
    (268012, 'astana'),
    (268020, 'almaty'),
    (20242100, 'shimkent')
]


@attr.s
class Row:
    bin = attr.ib(default='')
    full_name_kz = attr.ib(default='')
    full_name_ru = attr.ib(default='')
    registration_date = attr.ib(default='')
    oked_1 = attr.ib(default='')
    activity_kz = attr.ib(default='')
    activity_ru = attr.ib(default='')
    oked_2 = attr.ib(default='')
    krp = attr.ib(default='')
    krp_name_kz = attr.ib(default='')
    krp_name_ru = attr.ib(default='')
    kato = attr.ib(default='')
    settlement_kz = attr.ib(default='')
    settlement_ru = attr.ib(default='')
    legal_address = attr.ib(default='')
    head_fio = attr.ib(default='')


class RCutUrlFile(luigi.Task):

    """ Generate actual url for rcut from stat.gov.kz API into file """

    name = luigi.Parameter()
    juridical_type = luigi.IntParameter()
    cities = luigi.ListParameter(default=[])
    statuses = luigi.ListParameter(default=[39354, 39355, 39356, 39358, 534829, 39359])
    kato_number = luigi.IntParameter(default=2153)

    def output(self):
        return luigi.LocalTarget(os.path.join(TMP_DIR, f'{self.name}.url'))

    def run(self):
        rcutparser = SgovRCutParser(self.statuses)
        url = rcutparser.get_url(self.juridical_type)
        append_file(self.output().path, url)


class CompaniesRCutUrl(luigi.ExternalTask):

    """ Provide file with url generated by
        stat.gov.kz API.
    """

    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(TMP_DIR, f'{self.name}.url'))


class RetrieveCompaniesRCutDataFile(luigi.Task):

    """ Download rcut zip and extract xls file"""

    name = luigi.Parameter()

    def requires(self):
        return CompaniesRCutUrl(name=self.name)

    def output(self):
        return luigi.LocalTarget(os.path.join(TMP_DIR, f'{self.name}.xlsx'))

    def run(self):
        # read url from file
        url = read_lines(self.input().path)[0]
        apath = os.path.join(TMP_DIR, f'{self.name}.zip')
        frmt = save_webfile(url, apath)
        unzip_one_file(apath, self.name)


@requires(RetrieveCompaniesRCutDataFile)
class ParseCompaniesRCut(luigi.Task):

    skiptop = luigi.TupleParameter(default=None)

    def output(self):
        return luigi.LocalTarget(os.path.join(TMP_DIR, f'{self.name}.csv'))

    def run(self):
        # TODO finish
        parse_excel_rect_area_to_csv(self.input().path, self.output().path,
                                     Row, skiptopnum=self.skiptop)


# class CompaniesEntrepreneursByRegionsRCutToFtp(ParseCompaniesRCut):
#     pass


@requires(ParseCompaniesRCut)
class GzipCompaniesRCutToFtp(GzipToFtp):
    pass


class RetrieveCompaniesRegularDataFiles(luigi.Task):
    url = luigi.Parameter(default=None)
    name = luigi.Parameter(default=None)
    url_params = "https://stat.gov.kz/api/general/get"
    url_regions_tmpl = "https://stat.gov.kz/api/klazz/{}/{}/ru"
    url_region_id_tmpl = "https://stat.gov.kz/api/sbr/expPortalResult/{}/ru"
    url_region_download_tmpl = "https://stat.gov.kz/api/sbr/download?bucket=SBR&guid={}"

    def _links(self):
        # step 1
        r = requests.get(self.url_params)
        _json = r.json()
        cls_kato_id = _json["clsKatoVersionId"]
        kato_parent_id = _json["katoParent"]["itemId"]

        # step 2
        r = requests.get(self.url_regions_tmpl.format(cls_kato_id, kato_parent_id))
        _json = r.json()
        _list = []
        for region in _json["list"]:
            _list.append(region["itemId"])

        # step 3
        res = []
        for _l in _list:
            r = requests.get(self.url_region_id_tmpl.format(_l))
            _json = r.json()
            res.append(self.url_region_download_tmpl.format(_json["obj"]["fileGuid"]))

        return res

    def output(self):
        fpaths = []
        links = self._links()

        # according number of links we build paths for
        # xls files, cause each zip file contains one xls file
        for i, _ in enumerate(links):
            fpath = os.path.join(TMP_DIR, f'{self.name}_{i}.xls')
            fpaths.append(fpath)

        return [luigi.LocalTarget(f) for f in fpaths]

    def run(self):
        links = self._links()
        for i, f in enumerate(self.output()):
            # set status
            self.set_status_message('Saving {}'.format(basename(f.path)))

            save_webfile(links[i], f.path)

            percent = round((i+1)*100/len(self.output()))
            self.set_progress_percentage(percent)


@requires(RetrieveCompaniesRegularDataFiles)
class ParseCompanies(luigi.Task):

    sheets = luigi.TupleParameter(default=None)
    skiptop = luigi.TupleParameter(default=None)

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))

    def run(self):
        for i, target in enumerate(self.input()):
            self.set_status_message('Parsing {}'.format(target.path))
            rows = parse(target.path, Row, skiprows=self.skiptop,
                         sheets=self.sheets)
            save_csvrows(self.output().path, [attr.astuple(r) for r in rows])

            percent = round((i + 1) * 100 / len(self.input()))
            self.set_progress_percentage(percent)


@requires(ParseCompanies)
class GzipCompaniesToFtp(GzipToFtp):
    pass


class Companies(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesToFtp(name='sgov_companies',
                                 skiptop=3)


class CompaniesEntrepreneursByRegions(luigi.WrapperTask):
    def requires(self):
        for r in rcut_entr:
            yield ParseCompaniesRCut(name=f'statgovkz_rcut_enterpr_{r[1]}', skiptop=2)


class CompaniesForeignBranches(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_{rcut_foreign_branches}',
                                     skiptop=2,
                                     ftp_directory=rcut_foreign_branches,
                                     monthly=True)


class CompaniesLegalBranches(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_{rcut_legal_branches}',
                                     skiptop=2,
                                     ftp_directory=rcut_legal_branches,
                                     monthly=True)


class CompaniesJointVentures(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_{rcut_joint_ventures}', skiptop=2,
                                     ftp_directory=rcut_joint_ventures,
                                     monthly=True)


class CompaniesLegalEntities(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_{rcut_legal_entities}', skiptop=2,
                                     ftp_directory=rcut_legal_entities,
                                     monthly=True)


class CompaniesEntrepreneurs(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_{rcut_entrepreneurs}', skiptop=2,
                                     ftp_directory=rcut_entrepreneurs,
                                     monthly=True)


class CompaniesActiveForeignBranches(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_active_{rcut_foreign_branches}',
                                     skiptop=2,
                                     ftp_directory=rcut_foreign_branches,
                                     monthly=True)


class CompaniesActiveLegalBranches(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_active_{rcut_legal_branches}',
                                     skiptop=2,
                                     ftp_directory=rcut_legal_branches,
                                     monthly=True)


class CompaniesActiveJointVentures(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_active_{rcut_joint_ventures}', skiptop=2,
                                     ftp_directory=rcut_joint_ventures,
                                     monthly=True)


class CompaniesActiveLegalEntities(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_active_{rcut_legal_entities}', skiptop=2,
                                     ftp_directory=rcut_legal_entities,
                                     monthly=True)


class CompaniesActiveEntrepreneurs(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesRCutToFtp(name=f'statgovkz_active_{rcut_entrepreneurs}', skiptop=2,
                                     ftp_directory=rcut_entrepreneurs,
                                     monthly=True)


if __name__ == '__main__':
    luigi.run()
