import luigi
from gql import Client, AIOHTTPTransport, RequestsHTTPTransport

from tasks.base import LoadingDataIntoCsvFile


class GraphQlParsing(LoadingDataIntoCsvFile):

    url = luigi.Parameter()
    headers = luigi.DictParameter(default=None)
    query = luigi.Parameter()

    def get_client(self):
        sample_transport = RequestsHTTPTransport(
            url=str(self.url),
            verify=False,
            retries=3,
            headers=self.headers,
        )
        # transport = AIOHTTPTransport(url=str(self.url), headers=self.headers, ssl=False)
        client = Client(transport=sample_transport, fetch_schema_from_transport=True)
        return client













