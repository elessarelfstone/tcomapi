import luigi
from luigi.parameter import ParameterVisibility
from gql import Client
from gql.transport.requests import RequestsHTTPTransport

from tasks.base import LoadingDataIntoCsvFile, BigDataToCsv
from settings import GOSZAKUP_GQL_TOKEN


class GraphQlParsing(LoadingDataIntoCsvFile):

    url = luigi.Parameter()
    # headers = luigi.DictParameter(default=dict())
    query = luigi.Parameter(visibility=ParameterVisibility.HIDDEN)
    token = luigi.Parameter(default=GOSZAKUP_GQL_TOKEN)
    timeout = luigi.IntParameter(default=1)

    def get_client(self):
        headers = dict()
        headers['Authorization'] = self.token
        sample_transport = RequestsHTTPTransport(
            url=str(self.url),
            verify=False,
            retries=3,
            headers=headers,
        )
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


