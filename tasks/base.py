import json
import os
import shutil
from datetime import datetime, timedelta, date
from calendar import monthrange
from typing import Tuple


import attr
import luigi
from luigi.contrib.ftp import RemoteTarget
from luigi.util import requires

from tcomapi.dgov.api import (parse_addrreg, build_url_report,
                              load_versions, build_url_data, load_data,
                              QUERY_TMPL, CHUNK_SIZE)
from tcomapi.common.exceptions import ExternalSourceError
from tcomapi.common.utils import (build_fpath, build_webfpath, save_webfile,
                                  gziped_fname, gzip_file, date_for_fname, parsed_fpath,
                                  save_csvrows)

from tcomapi.common.unpacking import unpack
from settings import (TMP_DIR, ARCH_DIR, FTP_PATH,
                      FTP_HOST, FTP_USER, FTP_PASS, DGOV_API_KEY)


def prev_month(month: Tuple[int, int]) -> Tuple[int, int]:
    _year, _month = month
    first_day = date(_year, _month, 1)
    lastday_prevmonth = first_day - timedelta(days=1)
    return lastday_prevmonth.year, lastday_prevmonth.month


def month_to_range(month: str) -> Tuple[str, str]:
    _year, _month = map(int, month.split('-'))
    days = monthrange(_year, _month)[1]
    first_day = date(year=_year, month=_month, day=1)
    last_day = date(year=_year, month=_month, day=days)
    return first_day.strftime('%Y-%m-%d'), last_day.strftime('%Y-%m-%d')
    # return '{}:{}'.format(first_day.strftime('%Y-%m-%d'),
    #                       last_day.strftime('%Y-%m-%d'))

@attr.s
class DataList(object):
    data = attr.ib()


class BaseConfig(luigi.Config):
    @classmethod
    def name(cls):
        return cls.__name__.lower()


class RetrieveWebDataFile(luigi.Task):
    url = luigi.Parameter()
    name = luigi.Parameter()

    def output(self):
        fpath = os.path.join(TMP_DIR, self.name)
        return luigi.LocalTarget(fpath)

    def run(self):
        # download file and get format(rar, zip, xls, etc) of file
        frmt = save_webfile(self.url, self.output().path)


class RetrieveWebDataFileFromArchive(luigi.Task):
    url = luigi.Parameter()
    name = luigi.Parameter()
    fnames = luigi.ListParameter(default=None)

    @staticmethod
    def get_filepaths(folder, fnames):
        return [os.path.join(folder, fname) for fname in fnames]

    def output(self):
        fnames = self.fnames
        fpaths = self.get_filepaths(TMP_DIR, fnames)
        return [luigi.LocalTarget(f) for f in fpaths]

    def run(self):

        # build path for downloading file
        fpath = os.path.join(TMP_DIR, self.name)

        # download file and get format(rar, zip, xls, etc) of file
        frmt = save_webfile(self.url, fpath)
        unpack(fpath, frmt, [t.path for t in self.output()])


class GzipToFtp(luigi.Task):

    date = luigi.DateParameter(default=datetime.today())
    # fsizelim = luigi.IntParameter(default=0)

    def output(self):
        _ftp_path = os.path.join(FTP_PATH, gziped_fname(self.input().path,
                                                        suff=date_for_fname(self.date)))
        return RemoteTarget(_ftp_path, FTP_HOST,
                            username=FTP_USER, password=FTP_PASS)

    def run(self):
        if os.path.getsize(self.input().path) == 0:
            _fpath = os.path.join(ARCH_DIR, gziped_fname(self.input().path))
            self.output().put(_fpath, atomic=False)
        else:
            _fpath = gzip_file(self.input().path)
            arch_fpath = os.path.join(ARCH_DIR, os.path.basename(_fpath))
            shutil.copy(_fpath, arch_fpath)
            self.output().put(_fpath, atomic=False)


class ParseJavaScript(luigi.Task):

    url = luigi.Parameter(default='')
    name = luigi.Parameter(default='')
    pattern = luigi.Parameter(default='')

    def output(self):
        output_fpath = build_fpath(TMP_DIR, self.name, 'csv')
        return luigi.LocalTarget(output_fpath)


class ParseBigData(luigi.Task):

    name = luigi.Parameter(default='')

    def complete(self):
        res_fpath = build_fpath(TMP_DIR, self.name, 'success')
        if not os.path.exists(res_fpath):
            return False
        else:
            return True


class ParseBigElasticApi(ParseBigData):

    name = luigi.Parameter(default='')
    version = luigi.Parameter(default='')
    versions = luigi.TupleParameter(default='')
    rep_name = luigi.Parameter(default='')

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))


class ParseElasticApi(luigi.Task):

    name = luigi.Parameter(default='')
    version = luigi.Parameter(default='')
    versions = luigi.TupleParameter(default='')
    rep_name = luigi.Parameter(default='')
    struct = luigi.Parameter(default=None)

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))

    def run(self):
        query = '{' + QUERY_TMPL.format(0, CHUNK_SIZE) + '}'
        rep_url = build_url_report(self.rep_name)
        versions = self.versions
        if not versions:
            versions = load_versions(rep_url)
        for vs in versions:
            url = build_url_data(self.rep_name, DGOV_API_KEY,
                                 version=vs, query=query)
            data = load_data(url, self.struct)
            save_csvrows(self.output().path, data)


@requires(ParseElasticApi)
class GzipDataGovToFtp(GzipToFtp):
    pass


class ParseAddressRegister(ParseBigElasticApi):
    # version = luigi.Parameter(default='')
    # rep_name = luigi.Parameter(default='')
    struct = luigi.Parameter()
    updates_days = luigi.Parameter(default=None)

    def progress(self, status, percent):
        self.set_status_message(status)
        self.set_progress_percentage(percent)

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))

    def run(self):
        prs_fpath = parsed_fpath(self.output().path)
        updates_date = None
        if self.updates_days:
            days = float(self.updates_days)
            updates_date = datetime.today() - timedelta(days=days)

        parse_addrreg(self.rep_name, self.struct, DGOV_API_KEY, self.output().path, prs_fpath,
                      updates_date=updates_date.date(),
                      version=self.version, callback=self.progress)


@requires(RetrieveWebDataFile)
class ParseWebExcelFile(luigi.Task):

    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))


@requires(RetrieveWebDataFileFromArchive)
class ParseWebExcelFileFromArchive(luigi.Task):

    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))
