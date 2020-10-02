import luigi
from gql import Client, AIOHTTPTransport, RequestsHTTPTransport

from tasks.base import LoadingDataIntoCsvFile, BigDataToCsv


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


class GraphQlBigDataParsing(BigDataToCsv, LoadingDataIntoCsvFile):

    url = luigi.Parameter()
    headers = luigi.DictParameter(default=None)
    query = luigi.Parameter()
    limit = luigi.IntParameter(default=200)
    key = luigi.Parameter(default=None)

    def get_client(self):
        sample_transport = RequestsHTTPTransport(
            url=str(self.url),
            verify=False,
            retries=3,
            headers=self.headers,
        )
        client = Client(transport=sample_transport, fetch_schema_from_transport=True)
        return client


