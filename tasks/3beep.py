import fnmatch
from datetime import datetime, timedelta
from os.path import basename, exists, join

import attr

import luigi
from cassandra.cluster import Cluster, SimpleStatement
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import tuple_factory, named_tuple_factory
from luigi.util import requires
from luigi.contrib.ftp import RemoteTarget, RemoteFileSystem

from settings import (_3BEEP_CLUSTER_HOST, _3BEEP_CLUSTER_USER,
                      _3BEEP_CLUSTER_PASS, _3BEEP_CLUSTER_KEYSPACE, TMP_DIR,
                      FTP_IN_PATH, FTP_HOST, FTP_PATH,
                      FTP_PASS, FTP_USER, BIGDATA_TMP_DIR)

from tasks.base import LoadingDataIntoCsvFile, BaseRunner, GzipToFtp
from tcomapi.common.dates import LastPeriod
from tcomapi.common.dataflow import last_file_with_bins
from tcomapi.common.utils import (dict_to_csvrow, save_csvrows, build_fpath,
                                  read_file, fname_noext)

chat_table = 'prod_3beep.chat_rooms_messages'
session = None
cluster_hosts = []
fetch_size = 2000


class NoLastMessageIdToParseTaxPayments(Exception):
    pass


@attr.s
class ChatMessageRow:
    chat_room_id = attr.ib(default='')
    message_id = attr.ib(default='')
    message_author_id = attr.ib(default='')
    message_created_date_time = attr.ib(default='')
    message_text = attr.ib(default= lambda x: f'"{x}"')
    message_updated_date_time = attr.ib(default='')


def last_file_with_message_id(flist):
    dates = []
    for f in flist:
        fname = fname_noext(f)
        _dt = fname.split('.')[0].split('_')[6]
        dt = datetime.strptime(_dt, '%Y-%m-%d')
        dates.append((dt, f))

    dates.sort(key=lambda x: x[1])

    return dates[-1][1]


def prepare_session(host, user, password, keyspace):
    auth_provider = PlainTextAuthProvider(username=user,
                                          password=password)
    global session
    # cluster = Cluster(cluster_hosts.append(host), auth_provider=auth_provider)
    cluster = Cluster(['10.8.158.8', '10.8.158.9', '10.8.158.10'], auth_provider=auth_provider)
    # cluster = Cluster(['178.88.68.39'], auth_provider=auth_provider)
    session = cluster.connect(keyspace)
    session.row_factory = named_tuple_factory



def load_all():
    sql = """select  chat_room_id,
              message_id,
              message_author_id,
              message_created_date_time,
              message_text,
              message_updated_date_time """
    sql += f"from {chat_table} "
    return session.execute(SimpleStatement(sql, fetch_size=fetch_size))


def load(last_message_id):
    sql = """select  chat_room_id,
              message_id,
              message_author_id,
              message_created_date_time,
              message_text,
              message_updated_date_time """
    sql += f"from {chat_table} where message_id > {last_message_id} ALLOW FILTERING"
    return session.execute(SimpleStatement(sql, fetch_size=500))


class LastMessageID(luigi.ExternalTask):

    last_message_fname_patt = 'export_cassandra_3beep_last_message_id_*.csv'

    def output(self):
        rmfs = RemoteFileSystem(FTP_HOST, username=FTP_USER, password=FTP_PASS)

        files = None

        if rmfs.exists(FTP_IN_PATH):
            lst = rmfs.listdir(FTP_IN_PATH)
            files = fnmatch.filter([basename(l) for l in lst], self.last_message_fname_patt)
        else:
            NoLastMessageIdToParseTaxPayments('Could not find directory with bins')

        if not files:
            raise NoLastMessageIdToParseTaxPayments('Could not find any file with bins')

        # bins_fpath = join(FTP_IN_PATH, last_file_with_bins(files))
        bins_fpath = FTP_IN_PATH + '/' + last_file_with_message_id(files)
        return RemoteTarget(bins_fpath, FTP_HOST,
                            username=FTP_USER, password=FTP_PASS)


class ChatMessagesPeriodDataLoadingAll(LoadingDataIntoCsvFile):

    host = luigi.Parameter(default=_3BEEP_CLUSTER_HOST)
    user = luigi.Parameter(default=_3BEEP_CLUSTER_USER)
    password = luigi.Parameter(default=_3BEEP_CLUSTER_PASS)
    keyspace = luigi.Parameter(default=_3BEEP_CLUSTER_KEYSPACE)

    def run(self):

        prepare_session(self.host, self.user, self.password, self.keyspace)
        data, rows = [], []
        all_count = 0
        count = 0
        # ows = load_all()
        for row in load_all():
            rows.append(row)
            count += 1
            if count == fetch_size:
                data = [dict_to_csvrow(dict(d._asdict()), ChatMessageRow) for d in rows]
                save_csvrows(self.output().path, data)
                all_count += count
                count = 0
                self.set_status(f'Parsed: {all_count}', 0)
                rows = []


@requires(ChatMessagesPeriodDataLoadingAll)
class GzipChatMessagesPeriodDataAll(GzipToFtp):
    pass


class ChatMessagesAll(BaseRunner):

    def requires(self):
        # return ChatMessagesPeriodDataLoadingAll(
        return GzipChatMessagesPeriodDataAll(
            ftp_directory='cassandra_3beep',
            name='3beep_chat_messages_all',
            directory=TMP_DIR,
            struct=ChatMessageRow
            )


class ChatMessagesPeriodDataLoading(LoadingDataIntoCsvFile):

    dates_range = luigi.TupleParameter(default=None)

    host = luigi.Parameter(default=_3BEEP_CLUSTER_HOST)
    user = luigi.Parameter(default=_3BEEP_CLUSTER_USER)
    password = luigi.Parameter(default=_3BEEP_CLUSTER_PASS)
    keyspace = luigi.Parameter(default=_3BEEP_CLUSTER_KEYSPACE)

    @property
    def last_message_id(self):
        bids_fpath = build_fpath(self.directory, self.name, 'lmsgid')

        if not exists(bids_fpath):
            self.input().get(bids_fpath)

        return read_file(bids_fpath)

    def requires(self):
        return LastMessageID()

    def run(self):

        prepare_session(self.host, self.user, self.password, self.keyspace)
        rows = load(self.last_message_id)
        data = [dict_to_csvrow(dict(d._asdict()), self.struct) for d in rows]
        save_csvrows(self.output().path, data)


@requires(ChatMessagesPeriodDataLoading)
class GzipChatMessagesPeriodData(GzipToFtp):
    pass


class ChatMessages(BaseRunner):

    def requires(self):
        return GzipChatMessagesPeriodData(
            name='3beep_chat_messages',
            directory=TMP_DIR,
            ftp_directory='cassandra_3beep',
            struct=ChatMessageRow,
            dates_range=('2021-11-16 00:00:00.000', '2021-11-30 00:00:00.000')
            )


class ChatMessagesForYesterday(BaseRunner):

    @property
    def dates_range(self):
        yt = datetime.today() - timedelta(days=1)
        d_before = yt - timedelta(days=1)
        return d_before.strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    def requires(self):
        return GzipChatMessagesPeriodData(
            name='3beep_chat_messages',
            directory=TMP_DIR,
            ftp_directory='cassandra_3beep',
            struct=ChatMessageRow,
            dates_range=self.dates_range
            )


if __name__ == '__main__':
    luigi.run()
