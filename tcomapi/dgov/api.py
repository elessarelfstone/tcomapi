import json
from concurrent import futures
from datetime import datetime

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


def load_data_as_dict(url, struct, date=None):
    def date_from_str(s):
        pass

    _json = load_html(url, headers=headers)
    dicts = [{k.lower(): v for k, v in d.items()} for d in json.loads(_json)]
    # dicts = { for d in json.loads(_json)}
    _data = []
    for d in dicts:
        try:
            _data.append(attr.astuple(struct(**d)))
            # _data.append(attr.as(struct(**d)))
            # _
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

    def __init__(self, name, apikey):
        self.name = name
        self.apikey = apikey
        self.query_tmpl = '{"from": {}, "size": {}}'

    @staticmethod
    def build_filter(qfilter=None):
        if not qfilter:
            _filter = ''

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

        uri = '/api/detailed/{}/{}?apiKey={}'.format(rep_name, _v, apikey).replace('/?', '?')
        if query:
            uri = '{}&source={}'.format(uri, query)
        return '{}{}'.format(host, uri)

    @staticmethod
    def meta_url(host, rep_name, version):
        uri = '/meta/{}/{}'.format(rep_name, version).replace('//', '')
        return '{}{}'.format(host, uri)

    def process_slice(self, rep_name, start, size):
        url = ElasticApiParser.data_url(self.host, rep_name, self.apikey)
        query = '{"from": {}, "size": {}'.format(start, size)

        load_html()


    def process_rep(self, rep_name, elkfilter=None):
        def get_range(i, ss):
            pass

        ss = BIG_QUERY_SLICE_SIZE
        total = load_total(ElasticApiParser.detail_url(self.host, rep_name, self.apikey))
        req_cnt, rest = total // ss, total % ss
        # TODO divmod()
        if rest:
            req_cnt += 1

        to_do_map = [ (d * ss + 1, ss)  for d in range(req_cnt) ]


        with futures.ThreadPoolExecutor(max_workers=req_cnt) as executor:
            for rng in to_do_map:
                future = executor.submit()



        query = None
        if total > BIG_QUERY_SLICE_SIZE:
            query = self.query_tmpl.format(1, )




