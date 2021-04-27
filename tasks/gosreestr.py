import os
import fnmatch
from os.path import basename, exists


import luigi
from luigi.contrib.ftp import RemoteTarget, RemoteFileSystem
from luigi.util import requires

from tcomapi.common.exceptions import FtpDirectoryNotExists
from tcomapi.common.dataflow import last_file_with_bins
from tcomapi.common.utils import build_fname, read_lines, build_fpath, append_file
from tcomapi.gosreestr import gosreestr_parse_new_uids

from tasks.base import BaseTask, LoadingDataIntoFile
from settings import FTP_HOST, FTP_PASS, FTP_USER, FTP_PATH, FTP_IN_PATH, BIGDATA_TMP_DIR

class NoUidsToParseTaxPayments(Exception):
    pass


class GosreestrExistedUids(luigi.ExternalTask):

    bins_fname_tmp = 'export_gosreestr_uids_*.csv'

    def output(self):
        rmfs = RemoteFileSystem(FTP_HOST, username=FTP_USER, password=FTP_PASS)

        files = None

        if rmfs.exists(FTP_IN_PATH):
            lst = rmfs.listdir(FTP_IN_PATH)
            files = fnmatch.filter([basename(l) for l in lst], self.bins_fname_tmp)
        else:
            raise FtpDirectoryNotExists('Could not find directory with uids')

        if not files:
            raise NoUidsToParseTaxPayments('Could not find any file with bins')

        # bins_fpath = join(FTP_IN_PATH, last_file_with_bins(files))
        bins_fpath = FTP_IN_PATH + '/' + last_file_with_bins(files)
        return RemoteTarget(bins_fpath, FTP_HOST,
                            username=FTP_USER, password=FTP_PASS)


class ParseNewUids(LoadingDataIntoFile):

    success_fext = luigi.Parameter(default='success')
    timeout = luigi.IntParameter(default=420)
    timeout_error = luigi.IntParameter(default=900)

    @property
    def success_fpath(self):
        return build_fpath(self.directory, self.name, self.success_fext)

    def complete(self):
        if not os.path.exists(self.success_fpath):
            return False
        else:
            return True

    def requires(self):
        return GosreestrExistedUids()

    def output(self):
        directory = str(self.directory)
        fname = build_fname(self.name, 'uids', suff='new')
        return luigi.LocalTarget(os.path.join(directory, fname))

    def run(self):
        bids_fpath = build_fpath(self.directory, self.name, 'uids')

        # copy on local machine from ftp
        if not exists(bids_fpath):
            self.input().get(bids_fpath)

        uids = read_lines(bids_fpath)
        new_uids = gosreestr_parse_new_uids(self.output().path, uids, timeout=self.timeout,
                                            error_timeout=self.timeout_error,
                                            luigi_callback=self.set_status)
        append_file(self.success_fpath, len(new_uids))


class ParseNewUidsRunner(luigi.WrapperTask):
    def requires(self):
        yield ParseNewUids(
            name='gosreestr_companies',
            directory=BIGDATA_TMP_DIR
        )


if __name__ == '__main__':
    luigi.run()

