import os
import json
from collections import Counter, namedtuple, deque
from datetime import datetime
from time import sleep
from typing import List, Tuple

import attr
from bs4 import BeautifulSoup
from box import Box
from requests import Session, HTTPError, ConnectionError, Timeout, ReadTimeout
from requests.exceptions import RetryError


from tcomapi.common.exceptions import BadDataType, ExternalSourceError
from tcomapi.common.utils import (load_url_content, save_csvrows,
                                  get, append_file, success_fpath, load_content,
                                  get_stata, read_lines, dict_to_csvrow, apply_filter_to_dict)

HOST = 'https://data.egov.kz'


headers = {'user-agent': 'Apache-HttpClient/4.1.1 (java 1.5)'}
BACKOFF_FACTOR = 0.5
CHUNK_SIZE = 10000
RETRIES = 3
TIMEOUT = 3

URI_REPORT_PAGE_TEMPLATE = 'datasets/view?index={}'
URI_DATA_PAGE_TEMPLATE = '/api/v4/{}/{}?apiKey={}'
URI_DETAIL_PAGE_TEMPLATE = '/api/detailed/{}/{}?apiKey={}'
URI_META_PAGE_TEMPLATE = '/meta/{}/{}'
QUERY_TEMPLATE = '{"size":{},"query":{"range":{"modified":{"gte":"{}","lt":"{}"}}}}'

QUERY_TMPL = '"from":{},"size":{}'
RETRY_STATUS = (403, 500, 501, 502)

Chunk = namedtuple('Chunk', ['start', 'size', 'count'])
Chunk.__new__.__defaults__ = (0,) * len(Chunk._fields)


class ElkRequestError(Exception):
    pass


def load_versions(url):
    """ load versions of dataset"""
    html = load_url_content(url, headers=headers)
    soup = BeautifulSoup(html, 'lxml')
    datasets = soup.findAll("a", {'class': 'version'})
    return tuple([ds.text.strip() for ds in datasets])


def build_url_for_report_page(report_name):
    uri = URI_REPORT_PAGE_TEMPLATE.format(report_name)
    return '{}/{}'.format(HOST, uri)


def build_url_for_data_page(rep_name, apikey, version=None, query=None):
    _v = ''
    if version:
        _v = version

    uri = URI_DATA_PAGE_TEMPLATE.format(rep_name, _v, apikey).replace('//', '/')
    if query:
        uri = f'{uri}&source={query}'
    return f'{HOST}{uri}'


def build_url_for_detail_page(rep_name, apikey, version=None, query=None):
    _v = ''
    if version:
        _v = version

    uri = URI_DETAIL_PAGE_TEMPLATE.format(rep_name, _v, apikey).replace('/?', '?')
    if query:
        uri = '{}&source={}'.format(uri, query)
    return f'{HOST}{uri}'


def build_url_for_meta_page(rep_name, version):
    uri = URI_META_PAGE_TEMPLATE.format(rep_name, version).replace('//', '')
    return f'{HOST}{uri}'


def load_data(url, struct, column_filter=None):

    data = []

    try:
        r = load_content(url, headers=headers, timeout=TIMEOUT)

        # trasform to python object(dict, list)
        raw = json.loads(r)

        if isinstance(raw, dict):
            box_obj = Box(raw)
            if hasattr(box_obj, 'error'):
                # raise error if instead of data
                # we get error dict in response
                raise HTTPError(box_obj.error)

        for d in raw:
            try:
                # we need all values to be string

                _d = {k: str(v) for (k, v) in d.items()}
                # convert values in dict to str
                # for k, v in d.items():

                if column_filter:
                    _d = apply_filter_to_dict(_d, column_filter)

                data.append(dict_to_csvrow(_d, struct))
            except BadDataType as e:
                pass

    except (HTTPError, ReadTimeout) as e:
        raise ExternalSourceError('Could not load {}'.format(url))

    except BadDataType:
        pass

    return data


def check_updated(date_modified, check_date):

    _date = datetime.strptime(date_modified, '%Y-%m-%d %H:%M:%S')
    if _date > check_date:
        return True

    return False


def filter_updates(data, updates_for):
    _data = []
    for r in data:
        try:
            if datetime.strptime(r['modified'], '%Y-%m-%d %H:%M:%S') > updates_for:
                _data.append(r)
        except KeyError:
            print(r)
    return _data


def prepare_chunks(total: int, parsed_chunks: List[str]) -> Tuple[List[str], int, int]:

    # compute number of requests we'll need to do
    requests_count, rest = divmod(total, CHUNK_SIZE)
    if rest:
        requests_count += 1

    # build requests's(chunk) start points and their size
    chunks = []
    for i in range(requests_count):
        if i == 0:
            chunk = (0, str(CHUNK_SIZE+1), 0)
        else:
            chunk = (i * CHUNK_SIZE + 1, CHUNK_SIZE, 0)

        chunks.append(':'.join([str(p) for p in chunk]))

    parsed_count = 0
    if parsed_chunks:
        _chunks = set(chunks)
        _parsed_chunks = set(parsed_chunks)
        _chunks.difference_update(_parsed_chunks)
        chunks = list(_chunks)

        for ch in parsed_chunks:
            _ch = Chunk(*(':'.split(ch)))
            parsed_count += _ch.count

    return chunks, requests_count, parsed_count


def get_chunks_start_position(total_rows: int, chunk_size: int):

    # compute number of requests we'll need to do
    requests_count, rest = divmod(total_rows, CHUNK_SIZE)
    if rest:
        requests_count += 1

    chunks = [i * chunk_size + 1 for i in range(requests_count)]

    # we don't want lose first row
    # so for first chunk we start from 0
    # return [0] + chunks[1:]
    return chunks


def prepare_chunks2(chunks, parsed_chunks=None):

    if parsed_chunks:
        prepared = set(chunks)
        prepared.difference_update(set(parsed_chunks))
        return list(prepared)

    return chunks


def load2(url, struct, updates_date=None):
    def check_modified(m_date, c_date):
        m_date_format = '%Y-%m-%d %H:%M:%S'
        if datetime.strptime(m_date, m_date_format).date() > c_date:
            return True
        return False

    r = get(url, headers=headers, timeout=5)
    data = []

    # trasform to python object(dict, list)
    raw = json.loads(r)

    if isinstance(raw, dict):
        box_obj = Box(raw)
        if hasattr(box_obj, 'error'):
            # raise error if instead of data
            # we get error dict in response
            raise ElkRequestError(box_obj.error)

    # if we parse only updates, after given date
    if updates_date:
        for r in raw:
            m_date = Box(r, default_box=True).modified
            if m_date and check_modified(m_date, updates_date):
                data.append(dict_to_csvrow(r, struct))
        return data

    # data = [attr.astuple(struct(**d)) for d in raw]
    data = [dict_to_csvrow(d, struct) for d in raw]

    return data


def load3(url, struct):
    def check_modified(m_date, c_date):
        m_date_format = '%Y-%m-%d %H:%M:%S'
        if datetime.strptime(m_date, m_date_format).date() > c_date:
            return True
        return False

    r = get(url, headers=headers, timeout=5)
    data = []

    # trasform to python object(dict, list)
    raw = json.loads(r)

    if isinstance(raw, dict):
        box_obj = Box(raw)
        if hasattr(box_obj, 'error'):
            # raise error if instead of data
            # we get error dict in response
            raise ElkRequestError(box_obj.error)

    return [dict_to_csvrow(d, struct) for d in raw]


def prepare_callback_info(total, total_chunks, parsed_count, errors,
                          parsed_chunks_count, updates_after=None, is_retrying=False):
    upd_status = 'Updates after: {}.'.format(updates_after) if updates_after else ''
    retry_status = 'Retrying ...'.format(updates_after) if is_retrying else ''
    status = 'Total: {}. Parsed : {}. Errors: {}. {} {}'.format(total, parsed_count, errors,
                                                                upd_status, retry_status)
    percentage = int(round(parsed_chunks_count * 100/total_chunks))

    return status, percentage


def parse_dgovbig(rep, struct, apikey, output_fpath, parsed_fpath,
                  updates_date=None, version=None, query=None,
                  callback=None):

    # retriev total count
    total = load_total(build_url_for_detail_page(rep, apikey, version, query))

    # get parsed chunks from prs file
    parsed_chunks = []
    if os.path.exists(parsed_fpath):
        parsed_chunks = read_lines(parsed_fpath)

    is_retrying = False
    parsed_chunks_count = 0
    if parsed_chunks:
        parsed_chunks_count = len(parsed_chunks)
        is_retrying = True

    # build chunks considering already parsed chunks
    chunks, total_chunks, parsed_count = prepare_chunks(total, parsed_chunks)

    errors = 0

    # it's convinient having deque of chunks,
    # cause we can do retry, putting aside failed chunk for later
    chunks = deque(chunks)
    while chunks:
        _ch = chunks.popleft()
        chunk = Chunk(*(_ch.split(':')))
        query = '{' + QUERY_TMPL.format(chunk.start, chunk.size) + '}'

        url = build_url_for_data_page(rep, apikey, version=version, query=query)
        print(url)
        try:
            data = load2(url, struct, updates_date=updates_date)
        except (HTTPError, ConnectionError, Timeout, RetryError, ReadTimeout) as exc:
            chunks.append(_ch)
            sleep(TIMEOUT * 2)
            errors += 1
        else:
            _chunk = Chunk(chunk.start, chunk.size, len(data))
            parsed_count += _chunk.count
            parsed_chunks_count += 1
            save_csvrows(output_fpath, data)
            append_file(parsed_fpath, ':'.join((str(ch) for ch in _chunk)))
            sleep(TIMEOUT)
        if callback:
            s, p = prepare_callback_info(total, total_chunks,
                                         parsed_count, errors, parsed_chunks_count,
                                         updates_date, is_retrying)
            callback(s, p)

    # if we have not parsed all chunks
    # we shoud do retry after several time
    if total_chunks != parsed_chunks_count:
        raise ExternalSourceError("Could not parse all chunks. Try again.")

    stata = dict(total=total, parsed_count=parsed_count)
    append_file(success_fpath(output_fpath), json.dumps(stata))
    return parsed_count


def load_data_as_dict(url, struct, date=None):
    def date_from_str(s):
        pass

    _json = load_url_content(url, headers=headers)
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


def build_query(start_from: int,  size: int,
                dates_range=None) -> str:

    q = ''

    if dates_range:
        begin_day, end_day = dates_range
        q = ', "query":{"range":{"modified":{"gte":"%s","lt":"%s"}}}' \
                % (begin_day, end_day)

    return '{"from": %s, "size": %s %s}' % (start_from, size, q)


def load_total(det_url: str) -> int:
    cnt = 0
    r = get(det_url, headers=headers)
    if r:
        raw = json.loads(r)
        cnt = int(Box(raw).totalCount)

    return cnt


class DatagovApiParsing:
    def __init__(self, apikey, report_name, struct, chunk_size,
                 output_fpath, parsed_fpath=None, timeout=TIMEOUT):

        self.apikey = apikey
        self.report_name = report_name
        self.struct = struct
        self.chunk_size = chunk_size
        self.output_fpath = output_fpath
        self.parsed_fpath = parsed_fpath
        self.timeout = timeout
        self.total_rows_parsed = 0
        self.total_chunks_parsed = 0

        self.headers = headers

    def _progress_status_info(self, version, total_chunks, errors_count,
                              parsed_rows_count, parsed_chunks_count,
                              is_retrying=False, dates_range=None):

        status = f'Parsing report: {self.report_name}. Current version: {version}' + os.linesep

        if dates_range:
            status += f'Period: {dates_range[0]} - {dates_range[1]}' + os.linesep
        else:
            status += f'Period: all data' + os.linesep

        if is_retrying:
            status += 'Retrying...'

        status += f'Total chunks: {total_chunks}. Parsed chunks: {parsed_chunks_count}. Parsed rows: {parsed_rows_count}'
        progress = int(round(parsed_chunks_count * 100/total_chunks))

        return status, progress

    def get_total_rows_for_version(self, version):
        detail_page_url = build_url_for_detail_page(self.report_name, self.apikey,
                                                    version=version)

        dataurl = build_url_for_report_page(self.report_name)
        print(dataurl)
        print(detail_page_url)
        response = get(detail_page_url, headers=self.headers)

        raw_detail_page = json.loads(response)
        return int(Box(raw_detail_page).totalCount)

    def get_parsed_chunks(self):
        # get parsed chunks from prs file
        parsed_chunks = []
        if self.parsed_fpath and os.path.exists(self.parsed_fpath):
            parsed_chunks = read_lines(self.parsed_fpath)

        return [int(chunk) for chunk in parsed_chunks]

    def parse_report(self, version, dates_range=None, progress_callback=None):

        total_rows = self.get_total_rows_for_version(version)
        all_chunks = get_chunks_start_position(total_rows, self.chunk_size)

        # parsed_chunks = []

        # if self.parsed_fpath:
        parsed_chunks = self.get_parsed_chunks()

        is_retrying = False
        if parsed_chunks:
            is_retrying = True

        chunks = deque(prepare_chunks2(all_chunks, parsed_chunks))

        errors = 0
        parsed_chunks_count = len(parsed_chunks)
        parsed_rows_count = parsed_chunks_count * self.chunk_size

        while chunks:
            chunk = chunks.popleft()
            query = build_query(chunk, self.chunk_size, dates_range=dates_range)
            url = build_url_for_data_page(self.report_name, self.apikey,
                                          version=version, query=query)
            print(url)
            try:
                data = load3(url, self.struct)

            except (HTTPError, ConnectionError, Timeout, RetryError, ReadTimeout) as exc:
                chunks.append(chunk)
                sleep(TIMEOUT * 2)
                errors += 1
            else:

                if (not data) and dates_range:
                    break
                parsed_rows_count += len(data)
                parsed_chunks_count += 1
                save_csvrows(self.output_fpath, data)
                if self.parsed_fpath:
                    append_file(self.parsed_fpath, str(chunk))
                sleep(self.timeout)

            if progress_callback:
                s, p = self._progress_status_info(version, len(all_chunks),
                                                  errors, parsed_rows_count,
                                                  parsed_chunks_count,
                                                  is_retrying=is_retrying,
                                                  )
                progress_callback(s, p)

        if not total_rows:
            raise ExternalSourceError(f'Report {self.report_name}:{version} has no data. ')

        return total_rows, parsed_rows_count



