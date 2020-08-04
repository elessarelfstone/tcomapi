import os
import shutil
from datetime import datetime, timedelta, date
from calendar import monthrange
from typing import Tuple


import attr
import luigi
from luigi.contrib.ftp import RemoteTarget
from luigi.util import requires

from tcomapi.dgov.api import (parse_dgovbig, build_url_report,
                              load_versions, build_url_data, load_data,
                              QUERY_TMPL, CHUNK_SIZE)
from tcomapi.common.utils import (build_fpath, save_webfile,
                                  gziped_fname, gzip_file, date_for_fname, parsed_fpath,
                                  save_csvrows)

from tcomapi.common.unpacking import unpack
from settings import (BIGDATA_TMP_DIR, TMP_DIR, ARCH_DIR, FTP_PATH,
                      FTP_HOST, FTP_USER, FTP_PASS, DGOV_API_KEY)


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
        res_fpath = build_fpath(BIGDATA_TMP_DIR, self.name, 'success')
        if not os.path.exists(res_fpath):
            return False
        else:
            return True


class ParseBigElasticApi(ParseBigData):

    name = luigi.Parameter(default='')
    version = luigi.Parameter(default='')
    versions = luigi.TupleParameter(default='')
    rep_name = luigi.Parameter(default='')
    chunk_size = luigi.IntParameter(default=CHUNK_SIZE)
    api_key = luigi.Parameter(default=DGOV_API_KEY)
    struct = luigi.Parameter(default=None)
    columns_filter = luigi.DictParameter(default=None)

    def output(self):
        return luigi.LocalTarget(build_fpath(BIGDATA_TMP_DIR, self.name, 'csv'))


class ParseElasticApi(luigi.Task):

    name = luigi.Parameter(default='')
    version = luigi.Parameter(default='')
    versions = luigi.TupleParameter(default='')
    rep_name = luigi.Parameter(default='')
    chunk_size = luigi.IntParameter(default=CHUNK_SIZE)
    api_key = luigi.Parameter(default=DGOV_API_KEY)
    struct = luigi.Parameter(default=None)
    columns_filter = luigi.DictParameter(default=None)

    # def output(self):
    #     return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))

    def run(self):
        query = '{' + QUERY_TMPL.format(0, self.chunk_size) + '}'
        rep_url = build_url_report(self.rep_name)
        versions = self.versions
        if not versions:
            versions = load_versions(rep_url)
        for vs in versions:
            url = build_url_data(self.rep_name, self.api_key,
                                 version=vs, query=query)
            data = load_data(url, self.struct, self.columns_filter)
            save_csvrows(self.output().path, data)


@requires(ParseElasticApi)
class GzipDataGovToFtp(GzipToFtp):
    pass


class ParseDgovBig(ParseBigElasticApi):

    struct = luigi.Parameter()
    updates_days = luigi.Parameter(default=None)
    columns_filter = luigi.DictParameter(default=None)

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
            updates_date = updates_date.date()

        parse_dgovbig(self.rep_name, self.struct, self.api_key,
                      self.output().path, prs_fpath,
                      updates_date=updates_date,
                      version=self.version,
                      callback=self.progress)


@requires(ParseDgovBig)
class GzipDgovBigToFtp(GzipToFtp):
    pass


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
