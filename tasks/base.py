import os
import shutil
from datetime import datetime, timedelta, date
from calendar import monthrange
from typing import Tuple


import attr
import luigi
from luigi.contrib.ftp import RemoteTarget
from luigi.util import requires, inherits

from tcomapi.dgov.api import (parse_dgovbig, build_url_for_report_page,
                              load_versions, build_url_for_data_page, load_data,
                              QUERY_TMPL, CHUNK_SIZE)
from tcomapi.common.excel import parse_excel_rect_area_to_csv


from tcomapi.common.utils import (build_fpath, save_webfile,
                                  gziped_fname, gzip_file, date_for_fname, parsed_fpath,
                                  save_csvrows, build_fname)

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


class BaseTask(luigi.Task):
    """ Base root class for all tasks"""
    name = luigi.Parameter(default='')
    struct = luigi.Parameter(default=None)


class LoadingDataIntoFile(BaseTask):
    """ Base class for tasks that loads data into file"""
    directory = luigi.Parameter(default=None)


class LoadingDataIntoCsvFile(LoadingDataIntoFile):

    ext = luigi.Parameter(default='csv')
    sep = luigi.Parameter(default=';')

    def output(self):
        directory = str(self.directory)
        fname = build_fname(self.name, self.ext)
        return luigi.LocalTarget(os.path.join(directory, fname))

    def run(self):
        open(self.output().path, 'a', encoding="utf-8").close()


class WebDataFileDownloading(luigi.Task):
    url = luigi.Parameter()
    name = luigi.Parameter()

    def output(self):
        fpath = os.path.join(TMP_DIR, self.name)
        return luigi.LocalTarget(fpath)

    def run(self):
        # download file and get format(rar, zip, xls, etc) of file
        frmt = save_webfile(self.url, self.output().path)


@requires(WebDataFileDownloading)
class WebDataArchiveDownloadingAndUnpacking(luigi.Task):

    fnames = luigi.ListParameter(default=None)

    @staticmethod
    def get_filepaths(folder, fnames):
        return [os.path.join(folder, fname) for fname in fnames]

    def output(self):
        fpaths = self.get_filepaths(TMP_DIR, self.fnames)
        return [luigi.LocalTarget(f) for f in fpaths]

    def run(self):

        # # build path for downloading file
        arch_fpath = self.input().path
        #
        # # download file and get format(rar, zip, xls, etc) of file
        # frmt = save_webfile(self.url, arch_fpath)
        unpack(arch_fpath, [t.path for t in self.output()])


@requires(WebDataArchiveDownloadingAndUnpacking)
class WebDataExcelFileFromArchiveParsingToCsv(LoadingDataIntoCsvFile):

    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


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
        unpack(fpath, [t.path for t in self.output()])


class GzipToFtp(luigi.Task):

    date = luigi.DateParameter(default=datetime.today())
    ftp_directory = luigi.Parameter(default=None)
    monthly = luigi.Parameter(default=False)
    ftp_host = luigi.Parameter(default=FTP_HOST)
    ftp_user = luigi.Parameter(default=FTP_USER)
    ftp_pass = luigi.Parameter(default=FTP_PASS)
    ftp_path = luigi.Parameter(default=FTP_PATH)
    ftp_os_sep = luigi.Parameter(default='/')

    def output(self):
        # convert luigi.Parameter to string
        directory = str(self.ftp_directory)
        sep = str(self.ftp_os_sep)

        # build full path to target directory
        if self.ftp_directory:
            path = sep.join([self.ftp_path, directory])
        else:
            path = self.ftp_path

        dt = date_for_fname(self.date, for_month=self.monthly)
        fname = gziped_fname(self.input().path, suff=dt)
        ftp_path = sep.join([path, fname])
        return RemoteTarget(ftp_path, self.ftp_host,
                            username=self.ftp_user, password=self.ftp_pass)

    def run(self):
        _fpath = gzip_file(self.input().path)
        self.output().put(_fpath, atomic=False)

        # if os.path.getsize(self.input().path) == 0:
        #     _fpath = os.path.join(ARCH_DIR, gziped_fname(self.input().path))
        #     self.output().put(_fpath, atomic=False)
        # else:
        #     _fpath = gzip_file(self.input().path)
        #     arch_fpath = os.path.join(ARCH_DIR, os.path.basename(_fpath))
        #     shutil.copy(_fpath, arch_fpath)
        #     self.output().put(_fpath, atomic=False)


class ParseJavaScript(luigi.Task):

    url = luigi.Parameter(default='')
    name = luigi.Parameter(default='')
    pattern = luigi.Parameter(default='')

    def output(self):
        output_fpath = build_fpath(TMP_DIR, self.name, 'csv')
        return luigi.LocalTarget(output_fpath)


class BigDataToCsv(LoadingDataIntoCsvFile):

    # name = luigi.Parameter(default='')
    parsed_fext = luigi.Parameter(default='prs')
    success_fext = luigi.Parameter(default='success')

    @property
    def success_fpath(self):
        return build_fpath(self.directory, self.name, self.success_fext)

    @property
    def parsed_fpath(self):
        return build_fpath(self.directory, self.name, self.parsed_fext)

    @staticmethod
    def month_as_dates_range(month, frmt='%Y-%m-%d'):
        year = int(month[:4])
        month = int(month[-2:])
        start_date = date(year, month, 1).strftime(frmt)
        end_date = date(year, month, monthrange(year, month)[1]).strftime(frmt)
        return start_date, end_date

    def progress(self, status, percent):
        self.set_status_message(status)
        self.set_progress_percentage(percent)

    def complete(self):
        if not os.path.exists(self.success_fpath):
            return False
        else:
            return True


class ParseBigElasticApi(BigDataToCsv):

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
        rep_url = build_url_for_report_page(self.rep_name)
        versions = self.versions
        if not versions:
            versions = load_versions(rep_url)
        for vs in versions:
            url = build_url_for_data_page(self.rep_name, self.api_key,
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
        return luigi.LocalTarget(build_fpath(BIGDATA_TMP_DIR, self.name, 'csv'))

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


@requires(WebDataFileDownloading)
class WebExcelFileCustomParsingToCsv(LoadingDataIntoCsvFile):

    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))


@requires(WebDataFileDownloading)
class WebExcelFileParsingToCsv(LoadingDataIntoCsvFile):
    skiptop = luigi.IntParameter(default=None)
    skipbottom = luigi.IntParameter(default=None)
    usecolumns = luigi.Parameter(default=None)
    sheets = luigi.Parameter(default=None)
    transform_callback = luigi.Parameter(default=None)

    def run(self):
        super().run()
        parse_excel_rect_area_to_csv(self.input().path,
                                     self.output().path,
                                     self.struct,
                                     sheets=self.sheets,
                                     skiptopnum=self.skiptop,
                                     usecols=self.usecolumns,
                                     transform_callback=self.transform_callback
                                     )


@requires(RetrieveWebDataFileFromArchive)
class ParseWebExcelFileFromArchive(luigi.Task):

    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))


class BaseRunner(luigi.WrapperTask):
    pass


@BaseRunner.event_handler(luigi.Event.FAILURE)
def handler():
    pass
