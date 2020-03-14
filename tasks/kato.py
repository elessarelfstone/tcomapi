import os
from dataclasses import dataclass, astuple

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_to_csv
from settings import CONFIG_DIR
from tasks.base import (GzipToFtp, BaseConfig, ParseWebExcelFileFromArchive)

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


class sgov_kato(BaseConfig):
    url = luigi.Parameter(default='')
    fnames = luigi.TupleParameter()
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')


class KatoParse(ParseWebExcelFileFromArchive):
    def run(self):
        for target in self.input():
            rows = parse(target.path, Row, skiprows=self.skiptop, usecols=self.usecolumns)
            save_to_csv(self.output().path, [attr.astuple(r) for r in rows])


@requires(KatoParse)
class GzipKatoToFtp(GzipToFtp):
    pass


class Kato(luigi.WrapperTask):
    def requires(self):
        return GzipKatoToFtp(url=sgov_kato().url, fnames=sgov_kato().fnames,
                             name=sgov_kato().name(), skiptop=sgov_kato().skiptop,
                             usecolumns=sgov_kato().usecolumns)


if __name__ == '__main__':
    luigi.run()
