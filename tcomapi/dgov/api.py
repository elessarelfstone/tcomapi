import json

import attr
from bs4 import BeautifulSoup

from tcomapi.common.exceptions import BadDataType
from tcomapi.common.utils import load_html

headers = {'user-agent': 'Apache-HttpClient/4.1.1 (java 1.5)'}
BIG_QUERY_SLICE_SIZE = 10000


def load_versions(url):
    """ load versions of dataset"""
    html = load_html(url, headers=headers)
    soup = BeautifulSoup(html, 'lxml')
    datasets = soup.findAll("a", {'class': 'version'})
    return tuple([ds.text.strip() for ds in datasets])


def load_data_as_tuple(url, struct):
    _json = load_html(url, headers=headers)
    dicts = [{k.lower(): v for k, v in d.items()} for d in json.loads(_json)]
    _data = []
    for d in dicts:
        try:
            _data.append(attr.astuple(struct(**d)))
        except BadDataType as e:
            pass

    return _data
    # return [attr.astuple(struct(**_d)) for _d in data]


def build_query_url(url_template, dataset, api_key, begin=None, size=None):
    query = '{}'
    if begin:
        query = {"from": begin, "size": size}

    return url_template.format(dataset, api_key, str(query).replace("\'", '\"'))


def load_total(url):
    _json = load_html(url, headers=headers)
    return json.loads(_json)["totalCount"]


class ElasticApiParser:

    host = 'https://data.egov.kz'

    def __init__(self, url_template, struct, slice_size, qfilter=None):
        self.url_template = url_template
        self.slice_size = slice_size
        self.struct = struct
        self.qfilter = qfilter

    @staticmethod
    def build_filter(qfilter=None):
        if not qfilter:
            _filter = ''

    def process_slice(self, query):
        pass

    @staticmethod
    def report_url(host, rep_name):
        uri = 'datasets/view?index={}'.format(rep_name)
        return '{}/{}'.format(host, uri)

    @staticmethod
    def data_url(host, rep_name, apikey, version=None, query=None):
        _v = ''
        if version:
            _v = version

        uri = '/api/v4/{}/{}?apiKey={}'.format(rep_name, _v, apikey).replace('//', '')
        if query:
            uri = '{}&source={}'.format(uri, query)
        return '{}{}'.format(host, uri)

    @staticmethod
    def detail_url(host, rep_name, apikey, version=None, query=None):
        _v = ''
        if version:
            _v = version

        uri = '/api/detailed/{}/{}?apiKey={}'.format(rep_name, _v, apikey).replace('//', '')
        if query:
            uri = '{}&source={}'.format(uri, query)
        return '{}{}'.format(host, uri)

    @staticmethod
    def meta_url(host, rep_name, version):
        uri = '/meta/{}/{}'.format(rep_name, version).replace('//', '')
        return '{}{}'.format(host, uri)
