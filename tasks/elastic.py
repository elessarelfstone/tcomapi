import luigi
import os
from datetime import datetime
from luigi.util import requires


from settings import DGOV_API_KEY, BIGDATA_TMP_DIR
# from tasks.addressregister import SPbRow
from tasks.base import BigDataToCsv, LoadingDataIntoCsvFile, GzipToFtp
from tcomapi.dgov.api import DatagovApiParsing
from tcomapi.common.dates import month_as_dates_range
from tcomapi.common.utils import build_fname, build_fpath, append_file
from dates import default_month

CHUNK_SIZE = 10000


class ElasticApiParsing(luigi.Task):
    versions = luigi.TupleParameter()
    report_name = luigi.Parameter()
    chunk_size = luigi.IntParameter(default=CHUNK_SIZE)
    api_key = luigi.Parameter(default=DGOV_API_KEY)
    updates_dates_range = luigi.TupleParameter(default=None)


class SimpleElasticApiParsingToCsv(LoadingDataIntoCsvFile, ElasticApiParsing):

    def run(self):
        parser = DatagovApiParsing(self.api_key, self.report_name, self.struct,
                                   self.chunk_size, self.output().path)
        versions = tuple(self.versions)
        for ver in versions:
            parser.parse_report(ver)


class BigDataElasticApiParsingToCsv(BigDataToCsv, ElasticApiParsing):

    directory = luigi.Parameter(default=BIGDATA_TMP_DIR)

    def run(self):
        parser = DatagovApiParsing(self.api_key, self.report_name, self.struct,
                                   self.chunk_size, self.output().path,
                                   self.parsed_fpath)

        versions = tuple(self.versions)
        stat = ''
        for ver in versions:
            s = parser.parse_report(ver,
                                    dates_range=self.updates_dates_range,
                                    progress_callback=self.progress
                                    )
            stat += str(s) + os.linesep

        append_file(self.success_fpath, stat)


# class AddrRegSpb2(luigi.WrapperTask):
#
#     month = luigi.Parameter(default=default_month())
#
#     def requires(self):
#
#         month_range = month_as_dates_range(self.month, '%Y-%m-%d %H:%M:%S')
#
#         return BigDataElasticApiParsingToCsv(name='dgov_addrregspb',
#                                              struct=SPbRow,
#                                              directory=BIGDATA_TMP_DIR,
#                                              versions=('data',),
#                                              report_name='s_pb',
#                                              updates_dates_range=month_range
#                                              )


@requires(BigDataElasticApiParsingToCsv)
class GzipElasticApiParsingToCsv(GzipToFtp):
    pass


class AddrRegSpbForMonth(luigi.WrapperTask):

    month = luigi.Parameter(default=default_month())

    def requires(self):

        month_range = month_as_dates_range(self.month, '%Y-%m-%d %H:%M:%S')

        return GzipElasticApiParsingToCsv(name='dgov_addrregspb',
                                          struct=SPbRow,
                                          monthly=True,
                                          versions=('data',),
                                          report_name='s_pb',
                                          updates_dates_range=month_range)


if __name__ == '__main__':
    luigi.run()