import json
from collections import Counter
from concurrent import futures
from datetime import datetime

import attr
from bs4 import BeautifulSoup
from box import Box
from requests import Session, HTTPError, ConnectionError, Timeout
from retry_requests import retry, TSession


from tcomapi.common.exceptions import BadDataType
from tcomapi.common.utils import load_html, save_to_csv, get, append_file

HOST = 'https://data.egov.kz'


headers = {'user-agent': 'Apache-HttpClient/4.1.1 (java 1.5)'}
BACKOFF_FACTOR = 0.5
CHUNK_SIZE = 10000
RETRIES = 5
TIMEOUT = 3

URI_REP_TMPL = 'datasets/view?index={}'
URI_DATA_TMPL = '/api/v4/{}/{}?apiKey={}'
URI_DETAIL_TMPL = '/api/detailed/{}/{}?apiKey={}'
URI_META_TMPL = '/meta/{}/{}'

QUERY_TMPL = '"from": {}, "size": {}'


class ElkRequestError(Exception):
    pass


def load_versions(url):
    """ load versions of dataset"""
    html = load_html(url, headers=headers)
    soup = BeautifulSoup(html, 'lxml')
    datasets = soup.findAll("a", {'class': 'version'})
    return tuple([ds.text.strip() for ds in datasets])


def report_url(rep_name):
    """ Get url for """
    uri = URI_REP_TMPL.format(rep_name)
    return '{}/{}'.format(HOST, uri)


def data_url(rep_name, apikey, version=None, query=None):
    _v = ''
    if version:
        _v = version

    uri = URI_DATA_TMPL.format(rep_name, _v, apikey).replace('//', '')
    if query:
        uri = '{}&source={}'.format(uri, query)
    return '{}{}'.format(HOST, uri)


def detail_url(rep_name, apikey, version=None, query=None):
    _v = ''
    if version:
        _v = version

    uri = URI_DETAIL_TMPL.format(rep_name, _v, apikey).replace('/?', '?')
    if query:
        uri = '{}&source={}'.format(uri, query)
    return '{}{}'.format(HOST, uri)


def meta_url(rep_name, version):
    uri = URI_META_TMPL.format(rep_name, version).replace('//', '')
    return '{}{}'.format(HOST, uri)


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


def load(url, struct, timeout=TIMEOUT,
         retries=RETRIES,
         backoff_factor=BACKOFF_FACTOR):
    # make retry mechanism
    # tsession = TSession(timeout)
    # tsession.headers.update(headers)
    # sess = retry(tsession, retries=retries,
    #              backoff_factor=backoff_factor)
    # r = sess.get(url)
    #
    # if r.status_code != 200:
    #     r.raise_for_status()
    r = get(url, headers=headers, timeout=TIMEOUT,
            retries=RETRIES, backoff_factor=BACKOFF_FACTOR)

    data = None
    if r:
        # trasform to python object
        raw = json.loads(r)
        # OOP style of dict
        box_obj = Box(raw)
        if hasattr(box_obj, 'error'):
            # raise error if instead of data we get error dict
            # in response
            print(url)
            raise ElkRequestError(box_obj.error)
        else:
            data = [attr.astuple(struct(**d)) for d in raw]

    return data


def parse_chunk(url, struct, output_fpath,
                timeout=None, retries=None,
                backoff_factor=None):
    data = []
    try:
        data = load(url, struct, timeout=timeout,
                    retries=retries,
                    backoff_factor=backoff_factor)
    except Exception:
        raise
    else:
        save_to_csv(output_fpath)

    return len(data)


def parse_report(rep, struct, apikey, output_fpath, parsed_fpath,
                 parsed_chunks=None, version=None, query=None,
                 timeout=None, retries=None,
                 backoff_factor=None, callback=None):

    # initialize statistic
    stat = Counter()
    for i in ['success', 'serror', 'elkerror']:
        stat.setdefault(i, 0)

    d_url = detail_url(rep, apikey, version, query)
    total = load_total(d_url)

    # compute requests number we'll need to do
    req_cnt, rest = divmod(total, CHUNK_SIZE)
    if rest:
        req_cnt += 1

    # build requests start points and their size
    chunks = []
    for i in range(req_cnt):
        if i == 0:
            chunks.append((0, CHUNK_SIZE + 1))
        else:
            chunks.append((i * CHUNK_SIZE + 1, CHUNK_SIZE))

    # exclude already parsed chunks
    if parsed_chunks:
        _chunks = set(chunks)
        _chunks.difference_update(set(parsed_chunks))
        chunks = list(_chunks)

    with futures.ThreadPoolExecutor(max_workers=5) as ex:
        to_do_map = {}
        for chunk in chunks:
            # build query using chunk
            # _query = str(dict(start=chunk[0], size=chunk[1])).replace('start', 'from')
            # _query = QUERY_TMPL.format(chunk[0], chunk[1])
            _query = '{'+QUERY_TMPL.format(chunk[0], chunk[1])+'}'

            # build url
            url = data_url(rep, apikey, version=version, query=_query)
            future = ex.submit(parse_chunk, url, struct, output_fpath,
                               timeout, retries, backoff_factor)

            to_do_map[future] = chunk

        done_iter = futures.as_completed(to_do_map)

        all_parsed = 0
        for future in done_iter:
            try:
                parsed_count = future.result()

            except ElkRequestError:
                stat['elkerror'] += 1
                raise

            except (HTTPError, ConnectionError, Timeout) as exc:
                stat['serror'] += 1
                raise
            else:
                stat['success'] += 1
                start, size = to_do_map[future]
                append_file(parsed_fpath, '{},{}'.format(start, size))

            all_parsed += parsed_count
            if callback:
                callback(total, all_parsed)

    return all_parsed


def load_data_as_dict(url, struct, date=None):
    def date_from_str(s):
        pass

    _json = load_html(url, headers=headers)
    dicts = [{k.lower(): v for k, v in d.items()} for d in json.loads(_json)]
    _data = []
    for d in dicts:
        try:
            _data.append(attr.astuple(struct(**d)))
        except BadDataType as e:
            pass

    return _data


def build_query_url(url_template, dataset, api_key, begin=None, size=None):
    query = '{}'
    if begin:
        query = {"from": begin, "size": size}

    return url_template.format(dataset, api_key, str(query).replace("\'", '\"'))


def load_total(det_url: str) -> int:
    cnt = 0
    r = get(det_url, headers=headers)
    if r:
        raw = json.loads(r)
        cnt = int(Box(raw).totalCount)

    return cnt


class ElasticApiParser:

    host = 'https://data.egov.kz'
    query_tmpl = '{"from": {}, "size": {}}'

    def __init__(self, name, apikey):
        self.name = name
        self.apikey = apikey

    @staticmethod
    def build_filter(qfilter=None):
        if not qfilter:
            _filter = ''

    def process_slice(self, rep_name, start=None, size=None):
        if start:
            _
        url = ElasticApiParser.data_url(self.host, rep_name, self.apikey)
        query = '{"from": {}, "size": {}'.format(start, size)

        load_html()

    def process_rep(self, rep_name, processed=None, elkfilter=None):

        slice_size = BIG_QUERY_SLICE_SIZE
        total = load_total(detail_url(self.host, rep_name, self.apikey))
        req_cnt, rest = divmod(total, slice_size)
        if rest:
            req_cnt += 1

        to_do_map = [self.process_slice(d * slice_size + 1, slice_size) for d in range(req_cnt)]

        with futures.ThreadPoolExecutor(max_workers=req_cnt) as executor:
            for rng in to_do_map:
                future = executor.submit()

        query = None
        if total > BIG_QUERY_SLICE_SIZE:
            query = self.query_tmpl.format(1, )




