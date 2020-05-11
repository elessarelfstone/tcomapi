import os
from datetime import datetime, timedelta

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tasks.base import GzipToFtp, BaseConfig, ParseAddressRegister
from tcomapi.common.utils import parsed_fpath, read_lines, result_fpath
from tcomapi.dgov.api import parse_addrreg

from settings import CONFIG_DIR, DGOV_API_KEY


@attr.s
class SAtsRow:
    id = attr.ib(default='')
    rco = attr.ib(default='')
    name_rus = attr.ib(default='')
    name_kaz = attr.ib(default='')
    full_path_kaz = attr.ib(default='')
    full_path_rus = attr.ib(default='')
    d_ats_type_id = attr.ib(default='')
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
    d_room_type_code = attr.ib(default='')
    d_room_type_id = attr.ib(default='')
    full_path_rus = attr.ib(default='')
    full_path_kaz = attr.ib(default='')
    actual = attr.ib(default='')
    s_building_id = attr.ib(default='')
    number = attr.ib(default='')
    rca = attr.ib(default='')
    modified = attr.ib(default='')


class ParseAddrRegSpb(ParseAddressRegister):

    def run(self):
        prs_fpath = parsed_fpath(self.output().path)
        updates_date = None
        if self.updates_days:
            updates_date = datetime.today() - timedelta(days=self.updates_days)

        parse_addrreg(self.rep_name, SPbRow, DGOV_API_KEY, self.output().path, prs_fpath,
                      updates_date=updates_date.date(),
                      version=self.version, callback=self.progress)


class ParseAddressRegisterSAts(ParseAddressRegister):

    def run(self):
        prs_fpath = parsed_fpath(self.output().path)
        updates_date = None
        if self.updates_days:
            updates_date = datetime.today() - timedelta(days=self.updates_days)

        parse_addrreg(self.rep_name, SPbRow, DGOV_API_KEY, self.output().path, prs_fpath,
                      updates_date=updates_date.date(),
                      version=self.version, callback=self.progress)


@requires(ParseAddressRegister)
class GzipAddrRegToFtp(GzipToFtp):
    pass


class AddrRegSAts(luigi.WrapperTask):

    updates_days = luigi.IntParameter()
    rep_name = luigi.Parameter(default='s_ats')
    version = luigi.Parameter(default='data')
    struct = luigi.Parameter(default=SAtsRow)

    def requires(self):
        return GzipAddrRegToFtp(name='dgov_addrregsats',
                                struct=self.struct,
                                version=self.version,
                                rep_name=self.rep_name,
                                updates_days=self.updates_days)


class AddrRegSGeonims(luigi.WrapperTask):

    updates_days = luigi.IntParameter()
    rep_name = luigi.Parameter(default='s_geonims')
    version = luigi.Parameter(default='data')
    struct = luigi.Parameter(default=SGeonimsRow)

    def requires(self):
        return GzipAddrRegToFtp(name='dgov_addrregsgeonims',
                                struct=self.struct,
                                version=self.version,
                                rep_name=self.rep_name,
                                updates_days=self.updates_days)


class AddrRegSGrounds(luigi.WrapperTask):

    updates_days = luigi.IntParameter()
    rep_name = luigi.Parameter(default='s_grounds')
    version = luigi.Parameter(default='data')
    struct = luigi.Parameter(default=SGroundsRow)

    def requires(self):
        return GzipAddrRegToFtp(name='dgov_addrregsgrounds',
                                struct=self.struct,
                                version=self.version,
                                rep_name=self.rep_name,
                                updates_days=self.updates_days)


class AddrRegSBuildings(luigi.WrapperTask):

    updates_days = luigi.IntParameter()
    rep_name = luigi.Parameter(default='s_buildings')
    version = luigi.Parameter(default='data')
    struct = luigi.Parameter(default=SBuildingsRow)

    def requires(self):
        return GzipAddrRegToFtp(name='dgov_addrregsbuildings',
                                struct=self.struct,
                                version=self.version,
                                rep_name=self.rep_name,
                                updates_days=self.updates_days)


class AddrRegSpb(luigi.WrapperTask):

    updates_days = luigi.IntParameter()
    rep_name = luigi.Parameter(default='s_pb')
    version = luigi.Parameter(default='data')
    struct = luigi.Parameter(default=SPbRow)

    def requires(self):
        return GzipAddrRegToFtp(name='dgov_addrregspb',
                                struct=self.struct,
                                version=self.version,
                                rep_name=self.rep_name,
                                updates_days=self.updates_days)


if __name__ == '__main__':
    luigi.run()
