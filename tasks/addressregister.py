import os

import attr
import luigi
from luigi.configuration.core import add_config_path
from luigi.util import requires


from tasks.base import GzipToFtp, BaseConfig, ParseElasticApi
from tcomapi.common.utils import save_to_csv, append_file
from tcomapi.dgov.api import (load_datasets, load_data_as_tuple,
                              build_query_url, BIG_QUERY_SLICE_SIZE, load_total)

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
    url = luigi.Parameter(default='')
    url_data_template = luigi.Parameter(default='')
    url_total = luigi.Parameter(default='')
    datasets = luigi.TupleParameter()


config_path = os.path.join(CONFIG_DIR, 'addressregister.conf')
add_config_path(config_path)


class ParseAddressRegister(ParseElasticApi):

    def run(self):
        total = load_total(self.url_total)
        dataset = self.datasets[0]
        size = BIG_QUERY_SLICE_SIZE
        chunks = list(range(1, total, size))
        for i, start in enumerate(chunks):
            # set status
            self.set_status_message('Saving from {} to {} '.format(start, start + size-1))
            url = build_query_url(self.url_data_template, dataset, DGOV_API_KEY, start, size)
            data = load_data_as_tuple(url, Row)
            save_to_csv(self.output().path, data)
            percent = round((i+1)*100/len(chunks))
            self.set_progress_percentage(percent)


@requires(ParseAddressRegister)
class GzipMzpToFtp(GzipToFtp):
    pass


class AddressRegister(luigi.WrapperTask):

    def requires(self):
        return GzipMzpToFtp(url=dgov_addressregister().url, name=dgov_addressregister().name(),
                            url_data_template=dgov_addressregister().url_data_template,
                            url_total=dgov_addressregister().url_total,
                            datasets=dgov_addressregister().datasets)


if __name__ == '__main__':
    luigi.run()
