import re
import os
import urllib3
from os.path import basename
from shutil import move

import attr
import luigi
import requests
from luigi.configuration.core import add_config_path
from luigi.util import requires

from tcomapi.common.excel import parse
from tcomapi.common.utils import save_to_csv, save_webfile, build_fpath
from tcomapi.common.unpacking import unpack, zipo_flist
from settings import CONFIG_DIR, TMP_DIR
from tasks.base import GzipToFtp, BaseConfig

config_path = os.path.join(CONFIG_DIR, 'companies.conf')
add_config_path(config_path)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@attr.s
class Row:
    bin = attr.ib(default='')
    full_name_kz = attr.ib(default='')
    full_name_ru = attr.ib(default='')
    registration_date = attr.ib(default='')
    oked_1 = attr.ib(default='')
    activity_kz = attr.ib(default='')
    activity_ru = attr.ib(default='')
    oked_2 = attr.ib(default='')
    krp = attr.ib(default='')
    krp_name_kz = attr.ib(default='')
    krp_name_ru = attr.ib(default='')
    kato = attr.ib(default='')
    settlement_kz = attr.ib(default='')
    settlement_ru = attr.ib(default='')
    legal_address = attr.ib(default='')
    head_fio = attr.ib(default='')


class sgov_companies(BaseConfig):
    url = luigi.Parameter(default=None)
    template_url = luigi.Parameter(default=None)
    regex = luigi.Parameter(default=None)
    skiptop = luigi.IntParameter(default=0)
    skipbottom = luigi.IntParameter(default=0)
    usecolumns = luigi.Parameter(default='')
    sheets = luigi.TupleParameter(default=None)


class RetrieveCompaniesWebDataFiles(luigi.Task):
    url = luigi.Parameter(default=None)
    name = luigi.Parameter(default=None)
    template_url = luigi.Parameter(default=None)
    regex = luigi.Parameter(default=None)

    def _links(self):
        """ Parse links for all regions"""

        # we have page which contains all ids
        # for all region links, like ESTAT086119, etc
        _json = requests.get(self.url, verify=False).text
        ids = re.findall(self.regex, _json)

        # build links
        links = [f'{self.template_url}{_id}' for _id in ids]
        return links

    def output(self):
        fpaths = []
        links = self._links()

        # according number of links we build paths for
        # xls files, cause each zip file contains one xls file
        for i, link in enumerate(links):
            fpath = os.path.join(TMP_DIR, f'{self.name}_{i}.xls')
            fpaths.append(fpath)

        return [luigi.LocalTarget(f) for f in fpaths]

    def run(self):
        links = self._links()
        for i, f in enumerate(self.output()):

            # set status
            self.set_status_message('Saving {}'.format(basename(f.path)))

            # get just filename without extension
            bsname = basename(f.path).split('.')[0]

            # build path for each zip archive
            fpath = os.path.join(TMP_DIR, f'{bsname}.zip')
            save_webfile(links[i], fpath)
            zipo, flist = zipo_flist(fpath)

            folder = os.path.abspath(os.path.dirname(f.path))

            # extract single file
            zipo.extract(flist[0], folder)
            src = os.path.join(folder, flist[0])
            # rename xls file to file in output()
            move(src, f.path)

            percent = round((i+1)*100/len(self.output()))
            self.set_progress_percentage(percent)
            # progress_luigi_status(self, len(self.output()), i, f'Saving {}')


@requires(RetrieveCompaniesWebDataFiles)
class ParseCompanies(luigi.Task):

    sheets = luigi.TupleParameter(default=None)

    def output(self):
        return luigi.LocalTarget(build_fpath(TMP_DIR, self.name, 'csv'))

    def run(self):
        for target in self.input():
            sheets = self.sheets
            rows = parse(target.path, Row, skiprows=sgov_companies().skiptop,
                         sheets=sheets)
            save_to_csv(self.output().path, [attr.astuple(r) for r in rows])


@requires(ParseCompanies)
class GzipCompaniesToFtp(GzipToFtp):
    pass


class Companies(luigi.WrapperTask):
    def requires(self):
        yield GzipCompaniesToFtp(url=sgov_companies().url,
                                 name=sgov_companies().name(),
                                 template_url=sgov_companies().template_url,
                                 regex=sgov_companies().regex,
                                 sheets=sgov_companies().sheets)


if __name__ == '__main__':
    luigi.run()
