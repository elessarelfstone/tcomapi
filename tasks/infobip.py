import json
import os
from collections import namedtuple
from datetime import datetime, timedelta
from math import floor

import attr
import luigi
from box import Box
from luigi.util import requires

from tcomapi.common.dates import yesterday_as_str, DEFAULT_DATE_FORMAT
from tcomapi.infobip.parser import InfobipApiParser
from tcomapi.common.utils import dict_to_csvrow, save_csvrows, append_file, read_lines, read_file
from tasks.base import BigDataToCsv, GzipToFtp, ExternalLocalTarget
from settings import (TMP_DIR, BIGDATA_TMP_DIR, INFOBIP_API_URL,
                      INFOBIP_API_PASS, INFOBIP_API_USER, INFOBIP_API_TIMEOUT)


def default_corrector(value):
    if value is None:
        return ''
    return value


@attr.s
class InfobipAgentsRow:
    id = attr.ib(default='')
    displayname = attr.ib(default='')
    status = attr.ib(default='')
    availability = attr.ib(default='')
    role = attr.ib(default='')
    enabled = attr.ib(default='')
    createdat = attr.ib(default='')
    updatedat = attr.ib(default='')


@attr.s
class InfobipConversationRow:
    id = attr.ib(default='', converter=default_corrector)
    topic = attr.ib(default='', converter=default_corrector)
    summary = attr.ib(default='', converter=default_corrector)
    status = attr.ib(default='', converter=default_corrector)
    priority = attr.ib(default='', converter=default_corrector)
    queueid = attr.ib(default='', converter=default_corrector)
    agentid = attr.ib(default='', converter=default_corrector)
    createdat = attr.ib(default='', converter=default_corrector)
    updatedat = attr.ib(default='', converter=default_corrector)
    closedat = attr.ib(default='', converter=default_corrector)


@attr.s
class InfobipConvMessagesRow:
    id = attr.ib(default='')
    channel = attr.ib(default='')
    from_ = attr.ib(default='')
    to = attr.ib(default='')
    direction = attr.ib(default='')
    conversationid = attr.ib(default='')
    createdat = attr.ib(default='')
    updatedat = attr.ib(default='')
    contenttype = attr.ib(default='')


@attr.s
class InfobipConvTagsRow:
    conversationid = attr.ib(default='')
    name = attr.ib(default='')
    createdat = attr.ib(default='')
    updatedat = attr.ib(default='')


@attr.s
class InfobipQueueRow:
    id = attr.ib(default='', converter=default_corrector)
    name = attr.ib(default='', converter=default_corrector)
    createdat = attr.ib(default='', converter=default_corrector)
    updatedat = attr.ib(default='', converter=default_corrector)
    isautoassignmentenabled = attr.ib(default='', converter=default_corrector)
    stickyagenttimeoutdays = attr.ib(default='', converter=default_corrector)
    isstickyautoassignmentenabled = attr.ib(default='', converter=default_corrector)
    workingHoursid = attr.ib(default='', converter=default_corrector)
    deletedat = attr.ib(default='', converter=default_corrector)


class InfobipApiParsing(BigDataToCsv):

    entity = luigi.Parameter(default='')

    limit = luigi.IntParameter(default=100)
    dates_range = luigi.TupleParameter(default=None)

    user = luigi.Parameter(default=INFOBIP_API_USER)
    password = luigi.Parameter(default=INFOBIP_API_PASS)

    timeout = luigi.IntParameter(default=int(INFOBIP_API_TIMEOUT))

    def url(self):
        url = f'{INFOBIP_API_URL}{self.entity}'
        return url

    def prepare_dates(self):
        DatesRange = namedtuple('DatesRange', 'begin, end')
        d = DatesRange(*tuple(self.dates_range))
        begin = datetime.strptime(d.begin, DEFAULT_DATE_FORMAT)
        end = datetime.strptime(d.end, DEFAULT_DATE_FORMAT)
        end = end.replace(hour=23, minute=59, second=59)
        return begin.isoformat() + '.000UTC', end.isoformat() + '.000UTC'

    def params(self):

        # initializing parameters
        params = Box()
        params.page = 0
        params.limit = self.limit

        # parse for specific period
        if self.dates_range:
            params.updatedAfter, params.updatedBefore = self.prepare_dates()

        return params

    def run(self):

        params = self.params()
        parser = InfobipApiParser(self.url(), self.entity,
                                  self.timeout, self.user, self.password,
                                  params.to_dict())

        for rows in parser:
            data = [dict_to_csvrow(d, self.struct) for d in rows]
            save_csvrows(self.output().path, data)
            self.set_status(parser.status_message, parser.percent_done)

        append_file(self.success_fpath, 'good')


class InfobipAgentsToCsv(InfobipApiParsing):
    pass


@requires(InfobipAgentsToCsv)
class GzipInfobipAgentsToCsv(GzipToFtp):
    pass


class InfobipAgents(luigi.WrapperTask):
    def requires(self):

        return GzipInfobipAgentsToCsv(
            entity=f'agents',
            directory=TMP_DIR,
            ftp_directory='infobip',
            limit=999,
            name='infobip_agents',
            struct=InfobipAgentsRow
        )


class InfobipQueuesToCsv(InfobipApiParsing):
    pass


@requires(InfobipQueuesToCsv)
class GzipInfobipQueuesToCsv(GzipToFtp):
    pass


class InfobipQueues(luigi.WrapperTask):
    def requires(self):
        return GzipInfobipQueuesToCsv(
            entity='queues',
            directory=TMP_DIR,
            ftp_directory='infobip',
            limit=999,
            name='infobip_queues',
            struct=InfobipQueueRow
        )


class InfobipConversationsToCsv(InfobipApiParsing):
    pass


@requires(InfobipConversationsToCsv)
class GzipInfobipConversationsToCsv(GzipToFtp):
    pass


class InfobipConversations(luigi.WrapperTask):

    date = luigi.Parameter(default=yesterday_as_str(dt_format=DEFAULT_DATE_FORMAT))
    start_date = luigi.Parameter(default=date)

    def requires(self):

        return GzipInfobipConversationsToCsv(
            entity='conversations',
            directory=TMP_DIR,
            ftp_directory='infobip',
            # dates_range=(self.date, self.date),
            dates_range=('2022-01-03', '2022-01-26'),
            limit=999,
            name='infobip_conversations',
            struct=InfobipConversationRow
        )


class InfobipApiConversationsDetailsParsing(InfobipApiParsing):

    # buffers
    conversation_id = None
    failed_page = None
    total_parsed_count = 0

    def requires(self):
        return ExternalLocalTarget('infobip_conversations')

    def lock_data(self):

        data = Box()

        if os.path.exists(self.lock_fpath):
            # raw = read_file(self.lock_fpath)
            # raw = raw.replace("'", '"')
            # data = json.loads(raw)
            data = Box().from_json(filename=self.lock_fpath)

        return data

    def params(self):
        params = super().params()
        if self.entity == 'tags':
            params.conversationId = self.conversation_id

        if self.failed_page:
            params.page = self.failed_page

        return params

    def conversation_ids(self):
        # build list of conversations ids from
        # csv file with conversations assuming
        # it's first column in each row
        rows = read_lines(self.input().path)
        conv_ids = [row.split(self.sep)[0] for row in rows]

        # get meta data
        d = self.lock_data()

        last_id = None

        # get conversation id which was last id
        # we successfully parsed
        if d:
            last_id = d.last_id

        # slice rest of conversations ids
        # to get list of ids we haven't processed
        if last_id:
            conv_ids = conv_ids[conv_ids.index(last_id) + 1:]

        return conv_ids

    def status(self):
        # TODO implement override it with additional info about curr conv id and count of left ids ot parse
        return f'Current conversation ID:{self.conversation_id}. Total parsed count: {self.total_parsed_count}'

    def run(self):

        conversation_ids = self.conversation_ids()

        meta = self.lock_data()

        # page for last successfully parsed conversation
        if meta:
            self.failed_page = meta.page

        parsed_convs_count = 0
        parsed_total_items = 0
        conversations_total = len(conversation_ids)

        for c_id in conversation_ids:
            self.conversation_id = c_id
            params = self.params()
            parser = InfobipApiParser(self.url(), self.entity,
                                      self.timeout, self.user, self.password,
                                      params.to_dict())

            for rows in parser:
                _rows = []
                for d in rows:
                    _rows.append({**d, **{'conversationid': c_id}})

                data = [dict_to_csvrow(d, self.struct) for d in _rows]
                save_csvrows(self.output().path, data)
                meta.page, meta.last_id = parser.curr_page, c_id
                self.lock(meta.to_json())

                parsed_total_items += len(rows)
                status = f'Total conversation IDs: {len(conversation_ids)}.'
                status += f' Parsed conversation IDs: {parsed_convs_count} ' + '\n'
                status += f'Current conversation ID: {c_id}. Total parsed items: {parsed_total_items}. '
                status = status + '\n' + parser.status_message
                percentage = floor((parsed_convs_count * 100) / conversations_total)
                self.set_status(status, percentage)

            parsed_convs_count += 1

        self.success('good')


class InfobipApiMessagesParsing(InfobipApiConversationsDetailsParsing):

    def url(self):
        return f'{INFOBIP_API_URL}conversations/{self.conversation_id}/{self.entity}'


@requires(InfobipApiMessagesParsing)
class GzipInfobipApiMessagesParsing(GzipToFtp):
    pass


class InfobipMessages(luigi.WrapperTask):
    def requires(self):
        return GzipInfobipApiMessagesParsing(
            entity='messages',
            directory=TMP_DIR,
            ftp_directory='infobip',
            limit=999,
            name='infobip_messages',
            struct=InfobipConvMessagesRow
        )


class InfobipApiTagsParsing(InfobipApiConversationsDetailsParsing):
    pass


@requires(InfobipApiTagsParsing)
class GzipInfobipApiTagsParsing(GzipToFtp):
    pass


class InfobipTags(luigi.WrapperTask):
    def requires(self):
        return GzipInfobipApiTagsParsing(
            entity='tags',
            directory=TMP_DIR,
            ftp_directory='infobip',
            limit=999,
            name='infobip_tags',
            struct=InfobipConvTagsRow
        )


if __name__ == '__main__':
    luigi.run()







