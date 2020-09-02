import json
import os
from concurrent import futures
from time import sleep

from requests import Session, HTTPError, ConnectionError, Timeout, ReadTimeout
from requests.exceptions import RetryError

from tcomapi.common.exceptions import ExternalSourceError
from tcomapi.dgov.api import (TIMEOUT, QUERY_TMPL, Chunk, build_url_for_detail_page, build_url_for_data_page, load_total,
                              read_lines, prepare_chunks, load2, prepare_callback_info)
from tcomapi.common.utils import save_csvrows, append_file, success_fpath



def load(url, struct, updates_for=None, timeout=TIMEOUT,
         status_to_retry=RETRY_STATUS, retries=RETRIES,
         backoff_factor=BACKOFF_FACTOR):

    r = get(url, headers=headers, timeout=timeout,
            status_to_retry=status_to_retry,
            backoff_factor=backoff_factor)

    data = []

    # trasform to python object(dict, list)
    raw = json.loads(r)

    if isinstance(raw, dict):
        box_obj = Box(raw)
        if hasattr(box_obj, 'error'):
            # raise error if instead of data
            # we get error dict in response
            raise ElkRequestError(box_obj.error)
    else:
        # if updates_for:
        #     raw = filter_updates(raw, updates_for)

        data = [attr.astuple(struct(**d)) for d in raw]

    return data

def parse_chunk(url, struct, output_fpath, updates_for=None,
                timeout=None, retries=None,
                backoff_factor=None):
    data = []
    try:
        data = load(url, struct, updates_for=updates_for,
                    timeout=timeout, retries=retries,
                    backoff_factor=backoff_factor)
    except Exception:
        raise
    else:
        save_csvrows(output_fpath, data)
        # sleep(10)

    return len(data)


def parse_report(rep, struct, apikey, output_fpath, parsed_fpath,
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

    with futures.ThreadPoolExecutor(max_workers=3) as ex:
        to_do_map = {}

        for chunk in chunks:
            _chunk = Chunk(*(chunk.split(':')))
            query = query = '{' + QUERY_TMPL.format(_chunk.start, _chunk.size) + '}'

            url = build_url_for_data_page(rep, apikey, version=version, query=query)

            future = ex.submit(load2, url, struct, updates_date)

            to_do_map[future] = chunk

        done_iter = futures.as_completed(to_do_map)

        for future in done_iter:
            try:
                data = future.result()
                start, size, _ = to_do_map[future].split(':')
            except (HTTPError, ConnectionError, Timeout, RetryError, ReadTimeout) as exc:
                print(exc)
                errors += 1
                sleep(TIMEOUT * 2)
            else:

                _chunk = '{}:{}:{}'.format(start, size, len(data))
                print(_chunk)
                parsed_count += len(data)
                save_csvrows(output_fpath, data)
                append_file(parsed_fpath, _chunk)
                sleep(TIMEOUT)

            if callback:
                s, p = prepare_callback_info(total, total_chunks,
                                             parsed_count, errors, parsed_chunks_count,
                                             updates_date, is_retrying)
                callback(s, p)

    if total_chunks != parsed_chunks:
        raise ExternalSourceError("Could not parse all the data. Try again.")

    stata = dict(total=total, parsed_count=parsed_count)
    append_file(success_fpath(output_fpath), json.dumps(stata))
    return parsed_count
