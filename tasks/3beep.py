from datetime import datetime, timedelta

import luigi
import attr
from cassandra.cluster import Cluster, SimpleStatement
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import tuple_factory, named_tuple_factory
from luigi.util import requires

from settings import (_3BEEP_CLUSTER_HOST, _3BEEP_CLUSTER_USER,
                      _3BEEP_CLUSTER_PASS, _3BEEP_CLUSTER_KEYSPACE, TMP_DIR)

from tasks.base import LoadingDataIntoCsvFile, BaseRunner, GzipToFtp
from tcomapi.common.dates import LastPeriod
from tcomapi.common.utils import dict_to_csvrow, save_csvrows

chat_table = 'prod_3beep.chat_rooms_messages'
session = None
cluster_hosts = []


@attr.s
class ChatMessageRow:
    chat_room_id = attr.ib(default='')
    message_id = attr.ib(default='')
    message_author_id = attr.ib(default='')
    message_created_date_time = attr.ib(default='')
    message_text = attr.ib(default='')
    message_updated_date_time = attr.ib(default='')


def prepare_session(host, user, password, keyspace):
    auth_provider = PlainTextAuthProvider(username=user,
                                          password=password)
    global session
    # cluster = Cluster(cluster_hosts.append(host), auth_provider=auth_provider)
    cluster = Cluster(['178.88.68.39'], auth_provider=auth_provider)
    session = cluster.connect(keyspace)
    session.row_factory = named_tuple_factory


def load(dates_range):
    sql = """select  chat_room_id,
              message_id,
              message_author_id,
              message_created_date_time,
              message_text,
              message_updated_date_time """
    sql += f"from {chat_table} where message_created_date_time > '{dates_range[0]}' and message_created_date_time < '{dates_range[1]}' ALLOW FILTERING"
    print(sql)
    return session.execute(SimpleStatement(sql, fetch_size=500))


class ChatMessagesPeriodDataLoading(LoadingDataIntoCsvFile):

    dates_range = luigi.TupleParameter(default=None)

    host = luigi.Parameter(default=_3BEEP_CLUSTER_HOST)
    user = luigi.Parameter(default=_3BEEP_CLUSTER_USER)
    password = luigi.Parameter(default=_3BEEP_CLUSTER_PASS)
    keyspace = luigi.Parameter(default=_3BEEP_CLUSTER_KEYSPACE)

    def run(self):
        prepare_session(self.host, self.user, self.password, self.keyspace)
        rows = load(self.dates_range)
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

        return d_before.strftime('%d-%m-%Y %H:%M:%S'), datetime.today().strftime('%d-%m-%Y %H:%M:%S')

    def requires(self):
        print(self.dates_range)
        return GzipChatMessagesPeriodData(
            name='3beep_chat_messages',
            directory=TMP_DIR,
            ftp_directory='cassandra_3beep',
            struct=ChatMessageRow,
            dates_range=self.dates_range
            )

if __name__ == '__main__':
    luigi.run()
