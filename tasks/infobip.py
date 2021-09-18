import os
import json
from math import floor
from time import sleep
from datetime import datetime, timedelta

import attr
import luigi
import requests
from box import Box
from luigi.util import requires
from requests.auth import HTTPBasicAuth
from requests import Session

from tcomapi.common.constants import CSV_SEP
from tcomapi.common.dates import today_as_str, DEFAULT_DATE_FORMAT
from tcomapi.common.utils import dict_to_csvrow, save_csvrows, append_file, read_lines
from tasks.base import BigDataToCsv, GzipToFtp
from settings import TMP_DIR, BIGDATA_TMP_DIR, INFOBIP_API_URL, INFOBIP_API_PASS, INFOBIP_API_USER


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
    queueId = attr.ib(default='', converter=default_corrector)
    agentId = attr.ib(default='', converter=default_corrector)
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
    contentext = attr.ib(default='')
    contentype = attr.ib(default='')


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


class InfobipDictParsing(BigDataToCsv):

    uri = luigi.Parameter(default='')
    user = luigi.Parameter(default='')
    password = luigi.Parameter(default='')

    timeout = luigi.IntParameter(default=10)
    limit = luigi.IntParameter(default=100)

    parse_date = luigi.Parameter(default=None)

    def daily_params(self):
        t = datetime.fromisoformat(self.parse_date)
        before_iso = t.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + ".000UTC"
        t = t - timedelta(days=1)
        after_iso = t.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + ".000UTC"

        return f'updatedAfter={after_iso}&updatedBefore={before_iso}'

    def run(self):
        page = 0
        data = []
        cnt = 0
        if not self.parse_date:
            url = f'{INFOBIP_API_URL}{self.uri}?limit={self.limit}'
        else:
            url = f'{INFOBIP_API_URL}{self.uri}?limit={self.limit}&page={page}&{self.daily_params()}'

        entity = str(self.name).split('_')[1]

        while url:
            try:
                r = requests.get(url, timeout=self.timeout,
                                 auth=HTTPBasicAuth(self.user, self.password))

            except Exception:
                raise
            else:

                raw = r.json()
                raw_items = raw[entity]
                data = [dict_to_csvrow(d, self.struct) for d in raw_items]
                save_csvrows(self.output().path, data)
                page += 1
                if raw_items:
                    if not self.parse_date:
                        url = f'{INFOBIP_API_URL}{self.uri}?limit={self.limit}&page={page}'
                    else:
                        url = f'{INFOBIP_API_URL}{self.uri}?limit={self.limit}&page={page}&{self.daily_params()}'
                else:
                    url = None

        append_file(self.success_fpath, str(len(data)))


class InfobipMessagesParsing(BigDataToCsv):

    conv_uri = luigi.Parameter(default='conversations')
    tags_uri = luigi.Parameter(default='tags')
    messages_uri = luigi.Parameter(default='messages')

    def output(self):
        return [
            luigi.LocalTarget(os.path.join(self.directory, 'infobip_conversations.csv')),
            luigi.LocalTarget(os.path.join(self.directory, 'infobip_messages.csv')),
            luigi.LocalTarget(os.path.join(self.directory, 'infobip_tags.csv'))
        ]

    def run(self):

        conv_url_pattern = f'{INFOBIP_API_URL}{self.uri}?limit={self.limit}'
        conv_page = 0
        conv_url = f'{conv_url_pattern}&page={conv_page}'
        while conv_url:
            try:

                r = requests.get(conv_url, timeout=self.timeout,
                                 auth=HTTPBasicAuth(self.user, self.password))

            except Exception:
                raise
            else:
                pass


class InfobipAgentsToCsv(InfobipDictParsing):
    pass


@requires(InfobipAgentsToCsv)
class GzipInfobipAgentsToCsv(GzipToFtp):
    pass


class InfobipQueuesToCsv(InfobipDictParsing):
    pass


@requires(InfobipQueuesToCsv)
class GzipInfobipQueuesToCsv(GzipToFtp):
    pass


class InfobipConversationsToCsv(InfobipDictParsing):
    pass


@requires(InfobipConversationsToCsv)
class GzipInfobipConversationsToCsv(GzipToFtp):
    pass


class InfobipConvMessagesParsing(BigDataToCsv):

    parse_date = luigi.Parameter(default=None)

    user = luigi.Parameter(default='')
    password = luigi.Parameter(default='')

    timeout = luigi.IntParameter(default=4)
    limit = luigi.IntParameter(default=100)

    def add_contenttext(self, d):
        _d = d
        _d['contentext'] = _d.get('content').get('text')
        return _d

    def requires(self):
        return InfobipConversationsToCsv(
            directory=TMP_DIR,
            sep=';',
            uri='conversations',
            limit=999,
            name='infobip_conversations',
            struct=InfobipConversationRow,
            user=INFOBIP_API_USER,
            password=INFOBIP_API_PASS,
            parse_date=self.parse_date
        )

    def run(self):
        csv_rows = read_lines(self.input().path)
        conv_ids = [row.split(self.sep)[0] for row in csv_rows]

        last_id = None
        if os.path.exists(self.parsed_fpath):
            last_id = read_lines(self.parsed_fpath)[-1]

        if last_id:
            conv_ids = conv_ids[conv_ids.index(last_id)+1:]

        sz = len(conv_ids)
        for i, c_id in enumerate(conv_ids):
            page = 0
            url = f'{INFOBIP_API_URL}conversations/{c_id}/messages?limit={self.limit}'
            while url:
                try:

                    r = requests.get(url, timeout=self.timeout,
                                     auth=HTTPBasicAuth(self.user, self.password))

                except Exception:
                    raise
                else:
                    raw = r.json()
                    raw_items = raw['messages']
                    raw_items = [self.add_contenttext(r) for r in raw_items]
                    data = [dict_to_csvrow(d, self.struct) for d in raw_items]
                    save_csvrows(self.output().path, data)
                    append_file(self.parsed_fpath, c_id)
                    page += 1
                    if raw_items:
                        url = f'{INFOBIP_API_URL}conversations/{c_id}/messages?limit={self.limit}&page={page}'
                    else:
                        url = None

            self.set_status(c_id, floor((i * 100)/sz))
            sleep(self.timeout)

        append_file(self.success_fpath, str('good'))


@requires(InfobipConvMessagesParsing)
class GzipInfobipConvMessagesParsing(GzipToFtp):
    pass


class InfobipConvTagsParsing(BigDataToCsv):

    parse_date = luigi.Parameter(default=None)

    user = luigi.Parameter(default='')
    password = luigi.Parameter(default='')

    timeout = luigi.IntParameter(default=10)
    limit = luigi.IntParameter(default=100)

    def add_conv_id(self, d, conv_id):
        _d = d
        _d['conversationid'] = conv_id
        return _d

    def requires(self):
        return InfobipConversationsToCsv(
            directory=TMP_DIR,
            sep=';',
            uri='conversations',
            limit=999,
            name='infobip_conversations',
            struct=InfobipConversationRow,
            user=INFOBIP_API_USER,
            password=INFOBIP_API_PASS,
            parse_date=self.parse_date
        )

    def run(self):
        csv_rows = read_lines(self.input().path)
        conv_ids = [row.split(self.sep)[0] for row in csv_rows]

        for c_id in conv_ids:
            page = 0
            url = f'{INFOBIP_API_URL}tags?limit={self.limit}'
            while url:
                try:

                    r = requests.get(url, timeout=self.timeout,
                                     auth=HTTPBasicAuth(self.user, self.password))

                except Exception:
                    raise
                else:
                    raw = r.json()
                    raw_items = raw['tags']
                    raw_items = [self.add_conv_id(r, c_id) for r in raw_items]
                    data = [dict_to_csvrow(d, self.struct) for d in raw_items]
                    save_csvrows(self.output().path, data)
                    page += 1
                    if raw_items:
                        url = f'{INFOBIP_API_URL}tags?limit={self.limit}&page={page}'
                    else:
                        url = None

        append_file(self.success_fpath, str('good'))


@requires(InfobipConvTagsParsing)
class GzipInfobipConvTagsParsing(GzipToFtp):
    pass


class InfobipAgents(luigi.WrapperTask):
    def requires(self):
        return GzipInfobipAgentsToCsv(
            directory=TMP_DIR,
            ftp_directory='infobip',
            sep=';',
            uri='agents',
            limit=999,
            name='infobip_agents',
            struct=InfobipAgentsRow,
            user=INFOBIP_API_USER,
            password=INFOBIP_API_PASS
        )


class InfobipQueues(luigi.WrapperTask):
    def requires(self):
        return GzipInfobipQueuesToCsv(
            directory=TMP_DIR,
            ftp_directory='infobip',
            monthly=True,
            sep=';',
            uri='queues',
            limit=999,
            name='infobip_queues',
            struct=InfobipQueueRow,
            user=INFOBIP_API_USER,
            password=INFOBIP_API_PASS
        )


class InfobipConversations(luigi.WrapperTask):
    def requires(self):
        return GzipInfobipConversationsToCsv(
            directory=BIGDATA_TMP_DIR,
            ftp_directory='infobip',
            sep=';',
            uri='conversations',
            limit=999,
            name='infobip_conversations',
            struct=InfobipConversationRow,
            user=INFOBIP_API_USER,
            password=INFOBIP_API_PASS
        )


class InfobipConversationsForDate(luigi.WrapperTask):

    date = luigi.Parameter(today_as_str(dt_format=DEFAULT_DATE_FORMAT))

    def requires(self):
        return GzipInfobipConversationsToCsv(
            directory=TMP_DIR,
            ftp_directory='infobip',
            sep=';',
            uri='conversations',
            limit=999,
            name='infobip_conversations',
            struct=InfobipConversationRow,
            user=INFOBIP_API_USER,
            password=INFOBIP_API_PASS,
            parse_date=self.date
        )


class InfobipConvMessagesForDate(luigi.WrapperTask):

    date = luigi.Parameter(today_as_str(dt_format=DEFAULT_DATE_FORMAT))

    def requires(self):
        # return GzipInfobipConversationsToCsv(
        return GzipInfobipConvMessagesParsing(
            directory=BIGDATA_TMP_DIR,
            ftp_directory='infobip',
            sep=';',
            limit=999,
            name='infobip_messages',
            struct=InfobipConvMessagesRow,
            user=INFOBIP_API_USER,
            password=INFOBIP_API_PASS
            # parse_date=self.date
        )


class InfobipConvTagsForDate(luigi.WrapperTask):

    date = luigi.Parameter(today_as_str(dt_format=DEFAULT_DATE_FORMAT))

    def requires(self):
        # return GzipInfobipConversationsToCsv(
        return GzipInfobipConvTagsParsing(
            directory=TMP_DIR,
            ftp_directory='infobip',
            sep=';',
            limit=999,
            name='infobip_tags',
            struct=InfobipConvTagsRow,
            user=INFOBIP_API_USER,
            password=INFOBIP_API_PASS,
            parse_date=self.date
        )


if __name__ == '__main__':
    luigi.run()
