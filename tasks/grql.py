import luigi
from gql import Client, AIOHTTPTransport

from tasks.base import LoadingDataIntoCsvFile


class GraphQlParsing(LoadingDataIntoCsvFile):

    url = luigi.Parameter()
    headers = luigi.DictParameter(default=None)
    query = luigi.Parameter()

    def get_client(self):
        transport = AIOHTTPTransport(url=str(self.url), headers=self.headers, ssl=False)
        client = Client(transport=transport, fetch_schema_from_transport=True)
        return client













