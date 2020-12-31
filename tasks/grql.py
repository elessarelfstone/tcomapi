import luigi
from gql import Client, AIOHTTPTransport, RequestsHTTPTransport

from tasks.base import LoadingDataIntoCsvFile, BigDataToCsv
from settings import GOSZAKUP_TOKEN


class GraphQlParsing(LoadingDataIntoCsvFile):

    url = luigi.Parameter()
    # headers = luigi.DictParameter(default=dict())
    query = luigi.Parameter()
    token = luigi.Parameter(default=GOSZAKUP_TOKEN)
    timeout = luigi.IntParameter(default=60)

    def get_client(self):
        headers = dict()
        headers['Authorization'] = self.token
        sample_transport = RequestsHTTPTransport(
            url=str(self.url),
            verify=False,
            retries=3,
            headers=headers,
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


