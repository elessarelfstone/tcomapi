import os

import luigi

from settings import (BIGDATA_TMP_DIR, CH_MEDIATION_ARCH_HOST, CH_MEDIATION_ARCH_PORT,
                      CH_MEDIATION_ARCH_USER, CH_MEDIATION_ARCH_PASS)
from tcomapi.clickhouse import ClickhouseClient


class LoadSimpleBetweenDatesData(luigi.Task):

    table_name = luigi.Parameter(default=None)
    begin_date = luigi.DateParameter(default=None)
    end_date = luigi.DateParameter(default=None)

    def output(self):
        templated_fname = 'md_{}_range_{begin_date:%Y%m%d}_{end_date:%Y%m%d}.csv'
        instantiated_fname = templated_fname.format(self.table_name, begin_date=self.begin_date,
                                                    end_date=self.end_date)
        fname = f'md_{self.table_name}_range_{{date:%Y/%m/%d}}.csv'

        return luigi.LocalTarget(os.path.join(BIGDATA_TMP_DIR, instantiated_fname))

    def run(self):
        ch_client = ClickhouseClient(CH_MEDIATION_ARCH_HOST, CH_MEDIATION_ARCH_PORT,
                                     CH_MEDIATION_ARCH_USER, CH_MEDIATION_ARCH_PASS)

        query = ClickhouseClient.build_select_between_dates(self.table_name, 'event_date',
                                                            self.begin_date, self.end_date)

        ch_client.execute_client_command(query, self.output().path)