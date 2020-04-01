import json

import attr
from bs4 import BeautifulSoup

from tcomapi.common.utils import load_html

headers = {'user-agent': 'Apache-HttpClient/4.1.1 (java 1.5)'}
BIG_QUERY_SLICE_SIZE = 10000


def load_datasets(url):
    """ load versions of dataset"""
    html = load_html(url, headers=headers)
    soup = BeautifulSoup(html, 'lxml')
    datasets = soup.findAll("a", {'class': 'version'})
    return tuple([ds.text.strip() for ds in datasets])


def load_data_as_tuple(url, struct):
    _json = load_html(url, headers=headers)
    data = [{k.lower(): v for k, v in d.items()} for d in json.loads(_json)]
    return [attr.astuple(struct(**_d)) for _d in data]


def build_query_url(url_template, dataset, api_key, begin=None, size=None):
    query = '{}'
    if begin:
        query = {"from": begin, "size": size}

    return url_template.format(dataset, api_key, str(query).replace("\'", '\"'))


def load_total(url):
    _json = load_html(url, headers=headers)
    return json.loads(_json)["totalCount"]
