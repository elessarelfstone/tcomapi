import os
from datetime import datetime, timedelta

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tasks.base import GzipDataGovToFtp, GzipDgovBigToFtp, GzipToFtp
from tasks.elastic import BigDataElasticApiParsingToCsv, DATE_FORMAT
from tcomapi.common.dates import previous_month_as_str, month_as_dates_range
from tcomapi.common.utils import parsed_fpath, read_lines, success_fpath
from tcomapi.dgov.api import parse_dgovbig

from settings import CONFIG_DIR, DGOV_API_KEY


@attr.s
class DAtsTypes:
    actual = attr.ib(default='')
    code = attr.ib(default='')
    value_ru = attr.ib(default='')
    short_value_kz = attr.ib(default='')
    id = attr.ib(default='')
    short_value_ru = attr.ib(default='')
    value_kz = attr.ib(default='')


@attr.s
class DBuildingsPointers:
    actual = attr.ib(default='')
    code = attr.ib(default='')
    value_ru = attr.ib(default='')
    short_value_kz = attr.ib(default='')
    id = attr.ib(default='')
    short_value_ru = attr.ib(default='')
    value_kz = attr.ib(default='')


@attr.s
class DGeonimsTypes:
    actual = attr.ib(default='')
    code = attr.ib(default='')
    value_ru = attr.ib(default='')
    short_value_kz = attr.ib(default='')
    id = attr.ib(default='')
    short_value_ru = attr.ib(default='')
    value_kz = attr.ib(default='')


@attr.s
class DRoomsTypes:
    actual = attr.ib(default='')
    code = attr.ib(default='')
    value_ru = attr.ib(default='')
    short_value_kz = attr.ib(default='')
    id = attr.ib(default='')
    short_value_ru = attr.ib(default='')
    value_kz = attr.ib(default='')


@attr.s
class SAtsRow:
    id = attr.ib(default='')
    rco = attr.ib(default='')
    name_rus = attr.ib(default='')
    name_kaz = attr.ib(default='')
    full_path_kaz = attr.ib(default='')
    full_path_rus = attr.ib(default='')
    d_ats_type_id = attr.ib(default='')
    d_ats_type_code = attr.ib(default='')
    cato = attr.ib(default='')
    actual = attr.ib(default='')
    parent_id = attr.ib(default='')
    modified = attr.ib(default='')


@attr.s
class SGeonimsRow:
    id = attr.ib(default='')
    full_path_rus = attr.ib(default='')
    rco = attr.ib(default='')
    name_kaz = attr.ib(default='')
    full_path_kaz = attr.ib(default='')
    cato = attr.ib(default='')
    s_ats_id = attr.ib(default='')
    name_rus = attr.ib(default='')
    actual = attr.ib(default='')
    d_geonims_type_id = attr.ib(default='')
    parent_id = attr.ib(default='')
    modified = attr.ib(default='')


@attr.s
class SGroundsRow:
    id = attr.ib(default='')
    s_geonim_id = attr.ib(default='')
    full_path_rus = attr.ib(default='')
    full_path_kaz = attr.ib(default='')
    cadastre_number = attr.ib(default='')
    s_ats_id = attr.ib(default='')
    actual = attr.ib(default='')
    number = attr.ib(default='')
    rca = attr.ib(default='')
    modified = attr.ib(default='')


@attr.s
class SBuildingsRow:
    s_geonims_id = attr.ib(default='')
    d_buildings_pointer_id = attr.ib(default='')
    number = attr.ib(default='')
    modified = attr.ib(default='')
    full_path_rus = attr.ib(default='')
    id = attr.ib(default='')
    full_path_kaz = attr.ib(default='')
    distance = attr.ib(default='')
    this_is = attr.ib(default='')
    s_ats_id = attr.ib(default='')
    actual = attr.ib(default='')
    d_buildings_pointer_code = attr.ib(default='')
    parent_rca = attr.ib(default='')
    s_ground_id = attr.ib(default='')
    rca = attr.ib(default='')
    parent_id = attr.ib(default='')


@attr.s
class SPbRow:
    id = attr.ib(default='')
    d_room_type_id = attr.ib(default='')
    full_path_rus = attr.ib(default='')
    full_path_kaz = attr.ib(default='')
    actual = attr.ib(default='')
    s_building_id = attr.ib(default='')
    number = attr.ib(default='')
    rca = attr.ib(default='')
    modified = attr.ib(default='')


class AddrRegDAtsTypes(luigi.WrapperTask):

    def requires(self):
        return GzipDataGovToFtp(name='dgov_datstypes',
                                versions=('data',),
                                rep_name='d_ats_types',
                                struct=DAtsTypes)


class AddrRegDBuildingsPointers(luigi.WrapperTask):

    def requires(self):
        return GzipDataGovToFtp(name='dgov_dbuildingspointers',
                                versions=('data',),
                                rep_name='d_buildings_pointers',
                                struct=DBuildingsPointers)


class AddrRegDGeonimsTypes(luigi.WrapperTask):

    def requires(self):
        return GzipDataGovToFtp(name='dgov_dgeonimstypes',
                                versions=('data',),
                                rep_name='d_geonims_types',
                                struct=DGeonimsTypes)


class AddrRegDRoomsTypes(luigi.WrapperTask):

    def requires(self):
        return GzipDataGovToFtp(name='dgov_droomstypes',
                                versions=('data',),
                                rep_name='d_rooms_types',
                                struct=DRoomsTypes)


@requires(BigDataElasticApiParsingToCsv)
class GzipElasticApiParsingToCsv(GzipToFtp):
    pass


class AddrReg(luigi.WrapperTask):

    # month = luigi.Parameter(default=default_month())

    def requires(self):

        # month_range = month_as_dates_range(self.month, '%Y-%m-%d %H:%M:%S')

        return GzipElasticApiParsingToCsv(name='dgov_addrregsats',
                                          struct=SAtsRow,
                                          monthly=True,
                                          versions=('data',),
                                          report_name='s_ats')


class AddrRegSAts(luigi.WrapperTask):

    month = luigi.Parameter(default=previous_month_as_str())

    def requires(self):

        month_range = month_as_dates_range(self.month, DATE_FORMAT)

        return GzipElasticApiParsingToCsv(name='dgov_addrregsats',
                                          struct=SAtsRow,
                                          monthly=True,
                                          versions=('data',),
                                          report_name='s_ats',
                                          updates_dates_range=month_range)


class AddrRegSGeonims(luigi.WrapperTask):

    month = luigi.Parameter(default=previous_month_as_str())

    def requires(self):

        month_range = month_as_dates_range(self.month, DATE_FORMAT)

        return GzipElasticApiParsingToCsv(name='dgov_addrregsgeonims',
                                          struct=SGeonimsRow,
                                          monthly=True,
                                          versions=('data',),
                                          report_name='s_geonims',
                                          updates_dates_range=month_range)


class AddrRegSGrounds(luigi.WrapperTask):

    month = luigi.Parameter(default=previous_month_as_str())

    def requires(self):

        month_range = month_as_dates_range(self.month, DATE_FORMAT)

        return GzipElasticApiParsingToCsv(name='dgov_addrregsgrounds',
                                          struct=SGroundsRow,
                                          monthly=True,
                                          versions=('data',),
                                          report_name='s_grounds',
                                          updates_dates_range=month_range)


class AddrRegSBuildings(luigi.WrapperTask):

    month = luigi.Parameter(default=previous_month_as_str())

    def requires(self):

        month_range = month_as_dates_range(self.month, DATE_FORMAT)

        return GzipElasticApiParsingToCsv(name='dgov_addrregsbuildings',
                                          struct=SBuildingsRow,
                                          monthly=True,
                                          versions=('data',),
                                          report_name='s_buildings',
                                          updates_dates_range=month_range)


class AddrRegSpb(luigi.WrapperTask):

    month = luigi.Parameter(default=previous_month_as_str())

    def requires(self):

        month_range = month_as_dates_range(self.month, DATE_FORMAT)

        return GzipElasticApiParsingToCsv(name='dgov_addrregspb',
                                          struct=SPbRow,
                                          monthly=True,
                                          versions=('data',),
                                          report_name='s_pb',
                                          updates_dates_range=month_range)


if __name__ == '__main__':
    luigi.run()
