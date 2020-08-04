import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires
from typing import Dict

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_csvrows
from tcomapi.common.correctors import basic_corrector
from settings import CONFIG_DIR
from tasks.base import (GzipToFtp, BaseConfig, ParseWebExcelFileFromArchive, GzipDataGovToFtp,
                        GzipDgovBigToFtp, ParseDgovBig)

config_path = os.path.join(CONFIG_DIR, 'kato.conf')
add_config_path(config_path)


@attr.s
class Row:
    te = attr.ib(default='')
    ab = attr.ib(default='')
    cd = attr.ib(default='')
    ef = attr.ib(default='')
    hij = attr.ib(default='')
    k = attr.ib(default='')
    name_kaz = attr.ib(default='')
    name_rus = attr.ib(default='')
    nn = attr.ib(default='')


@attr.s
class RowDgovKato:
    code = attr.ib(default='')
    ab = attr.ib(default='')
    cd = attr.ib(default='')
    ef = attr.ib(default='')
    hij = attr.ib(default='')
    area_type = attr.ib(default='')
    name_kaz = attr.ib(default='')
    name_rus = attr.ib(default='')
    nn = attr.ib(default='')


class sgov_kato(BaseConfig):
    url = luigi.Parameter(default='')
    fnames = luigi.TupleParameter()
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class KatoSgovParse(ParseWebExcelFileFromArchive):
    def run(self):
        for target in self.input():
            rows = parse(target.path, Row, skiprows=self.skiptop, usecols=self.usecolumns)
            save_csvrows(self.output().path, [attr.astuple(r) for r in rows])


@requires(KatoSgovParse)
class GzipKatoToFtp(GzipToFtp):
    pass


class KatoDgovParse(luigi.WrapperTask):

    def requires(self):
        column_filter = {'NameKaz': 'name_kaz', 'NameRus': 'name_rus',
                         'Code': 'te', 'AreaType': 'k'}
        return GzipDataGovToFtp(name='dgov_kato',
                                versions=('data',),
                                rep_name='kato',
                                struct=Row,
                                columns_filter=column_filter)


class KatoDgovParse2(luigi.WrapperTask):

    # def requires(self):
    #     return GzipDgovBigToFtp(name='dgov_kato',
    #                             struct=RowDgovKato,
    #                             versions=('data',),
    #                             rep_name='kato')
    def requires(self):
        return ParseDgovBig(name='dgov_kato',
                            struct=RowDgovKato,
                            versions=('data',),
                            rep_name='kato')


class Kato(luigi.WrapperTask):
    def requires(self):
        return GzipKatoToFtp(url=sgov_kato().url, fnames=sgov_kato().fnames,
                             name=sgov_kato().name(), skiptop=sgov_kato().skiptop,
                             usecolumns=sgov_kato().usecolumns)


if __name__ == '__main__':
    luigi.run()
