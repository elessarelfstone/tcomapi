import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tasks.base import GzipToFtp, BaseConfig, ParseElasticApi
from tcomapi.common.utils import parsed_fpath, load_lines
from tcomapi.dgov.api import (load_versions, load_data_as_tuple,
                              build_query_url, CHUNK_SIZE, load_total,
                              parse_report)

from settings import CONFIG_DIR, DGOV_API_KEY


@attr.s
class Row:
    id = attr.ib(default='')
    d_room_type_code = attr.ib(default='')
    category_room = attr.ib(default='')
    d_room_type_id = attr.ib(default='')
    full_path_rus = attr.ib(default='')
    full_path_kaz = attr.ib(default='')
    actual = attr.ib(default='')
    s_building_id = attr.ib(default='')
    number = attr.ib(default='')
    rca = attr.ib(default='')
    rca_building = attr.ib(default='')
    modified = attr.ib(default='')


class dgov_addressregister(BaseConfig):
    rep_name = luigi.Parameter(default='')
    versions = luigi.TupleParameter(default=None)


config_path = os.path.join(CONFIG_DIR, 'addressregister.conf')
add_config_path(config_path)


class ParseAddressRegister(ParseElasticApi):

    def progress(self, total, parsed):
        status_message = ''
        self.set_status_message = '{} of {}'.format(parsed, total)

        self.set_progress_percentage = int(round(parsed * 100/total))

    def run(self):
        rep = self.rep_name
        out_fpath = self.output().path
        prs_fpath = parsed_fpath(self.output().path)

        parsed_chunks = []
        if os.path.exists(prs_fpath):
            for line in load_lines(prs_fpath):
                start, size = line.split(',')
                parsed_chunks.append((int(start), int(size)))

        parse_report(rep, Row, DGOV_API_KEY, out_fpath, prs_fpath,
                     parsed_chunks=parsed_chunks, version=self.versions[0], callback=self.progress)

        # total = load_total(self.url_total)
        # dataset = self.datasets[0]
        # size = CHUNK_SIZE
        # chunks = list(range(1, total, size))
        # for i, start in enumerate(chunks):
        #     # set status
        #     self.set_status_message('Saving from {} to {} '.format(start, start + size-1))
        #     url = build_query_url(self.url_data_template, dataset, DGOV_API_KEY, start, size)
        #     data = load_data_as_tuple(url, Row)
        #     save_to_csv(self.output().path, data)
        #     percent = round((i+1)*100/len(chunks))
        #     self.set_progress_percentage(percent)


@requires(ParseAddressRegister)
class GzipMzpToFtp(GzipToFtp):
    pass


class AddressRegister(luigi.WrapperTask):

    def requires(self):
        return GzipMzpToFtp(name=dgov_addressregister().name(),
                            versions=dgov_addressregister().versions,
                            rep_name=dgov_addressregister().rep_name)


if __name__ == '__main__':
    luigi.run()
