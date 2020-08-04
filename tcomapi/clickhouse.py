from tcomapi.common.utils import run_and_report


class ClickHouseException(Exception):
    pass


class ClickhouseClient:

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def build_client_command(self, query, output_fpath):
        command = f'clickhouse-client --host={self.host} ' \
            f'--user={self.user} --password={self.password} -q ' \
            f'"{query} FORMAT CSV" > {output_fpath}'

        return command

    @staticmethod
    def build_select_between_dates(table, date_field, begin_date, end_date):
        q = f'select * from {table} where {date_field} between {begin_date} and {end_date}'
        return q

    def execute_client_command(self, query, output_fpath):
        command = self.build_client_command(query, output_fpath)
        result = run_and_report([command])

        if result.lower('exception').find >= 0:
            raise ClickhouseClient(result)

        return result










