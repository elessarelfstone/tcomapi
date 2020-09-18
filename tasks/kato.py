import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires
from typing import Dict

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_csvrows
from tcomapi.common.correctors import basic_corrector
from settings import CONFIG_DIR, TMP_DIR
from tasks.base import (GzipToFtp, BaseConfig, ParseWebExcelFileFromArchive, GzipDataGovToFtp,
                        GzipDgovBigToFtp, ParseDgovBig, WebDataExcelFileFromArchiveParsingToCsv)

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
    code = attr.ib(converter=lambda x: str(x), default='')
    ab = attr.ib(default='')
    cd = attr.ib(default='')
    ef = attr.ib(default='')
    hij = attr.ib(default='')
    areatype = attr.ib(converter=lambda x: str(x), default='')
    namekaz = attr.ib(default='')
    namerus = attr.ib(default='')
    nn = attr.ib(default='')

    def __attrs_post_init__(self):
        self.code = str(self.code)
        self.ab = self.code[:2]
        self.cd = self.code[2:4]
        self.ef = self.code[4:6]
        self.hij = self.code[-3:]


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


class KatoSgovParse2(WebDataExcelFileFromArchiveParsingToCsv):
    def run(self):
        for target in self.input():
            rows = parse(target.path, Row, skiprows=self.skiptop, usecols=self.usecolumns)
            save_csvrows(self.output().path, [attr.astuple(r) for r in rows])


@requires(KatoSgovParse2)
class GzipKatoToFtp(GzipToFtp):
    pass


class KatoDgovParse(ParseDgovBig):
    pass


@requires(KatoDgovParse)
class GzipDgovKatoToFtp(GzipToFtp):
    pass


class DgovKato(luigi.WrapperTask):
    def requires(self):
        return GzipDgovKatoToFtp(monthly=True,
                                 name='dgov_kato',
                                 struct=RowDgovKato,
                                 version='data',
                                 rep_name='kato'
                                 )


class Kato(luigi.WrapperTask):
    def requires(self):
        return GzipKatoToFtp(directory=TMP_DIR,
                             url=sgov_kato().url,
                             fnames=sgov_kato().fnames,
                             name=sgov_kato().name(),
                             skiptop=sgov_kato().skiptop,
                             usecolumns=sgov_kato().usecolumns)


if __name__ == '__main__':
    luigi.run()
