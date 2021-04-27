import os
import json
from collections import deque
from time import sleep
from typing import List, Dict

import requests
import attr
from fake_headers import Headers
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from urllib3.exceptions import ReadTimeoutError

from tcomapi.common.utils import read_file, append_file, read_lines
from tcomapi.gosreestr.parsing import (check_empty_table, parse_ids_from_table,
                                       parse_company_info)

LIST_URL = 'https://gr5.gosreestr.kz/p/ru/gr-search/search-objects'
DETAIL_URL = 'https://gr5.gosreestr.kz/p/ru/GrObjects/objects/teaser-view/{}?OptionName=BlockGrObjectsExtraInformation'
# HEADERS = {"User-Agent":"Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Mobile Safari/537.36"}

FORM_DATA = {'pager-page-index_search-GrObjectsHeadRevisions': '0',
 '__RequestVerificationToken': '3pryUOiM7Njcl/5ikr8yzVfluNAZsgo53GgCphJ1Z3aCs5RZStR2q4b aI5ErKV6FktJh2Al54t2TZrYae0ivH9YJpe1DeugE3K4x9XCz/E5h3wK7gTm8uUjDDD X8R2GVuTbPvQL4fcvRcacDeBVuHamoM=',
 'query-structure-search-GrObjectsHeadRevisions':'<LogicGroup GroupOperatorValue="And" GroupOperatorText="И" ><Condition IsStaticCondition="True" FieldName="tbGrObjects_flBlock" FieldText="Блокировка" ConditionOperatorValue="In" ConditionOperatorText="Входит в" Value="" ValueText="Слияние; Банкротство; Свободно; Ликвидация; Реабилитация; Остаток; Продан и неоформлена продажа; Сегментация; Акционирование; Преобразование в гп; Перевод в коммунальную собственность; Перевод в номинальное держание; Перевод в республиканскую собственность; Преобразование в тоо; Залоговый фонд" /></LogicGroup>'}


@attr.s
class GosreestrCompany:
    business_id = attr.ib(default='', metadata={'label_key': 'flbin'})
    gosreest_id = attr.ib(default='', metadata={'label_key': 'flgrid'})
    rnn = attr.ib(default='', metadata={'label_key': 'flrnn'})
    okpo = attr.ib(default='', metadata={'label_key': 'flbin'})
    name_ru = attr.ib(default='', metadata={'label_key': 'flnameru'})
    name_kz = attr.ib(default='', metadata={'label_key': 'flnamekz'})
    opf = attr.ib(default='', metadata={'label_key': 'flbin'})
    kfsl3 = attr.ib(default='', metadata={'label_key': 'flkfsl3'})


def get_mapping_from_struct(struct: GosreestrCompany) -> Dict:
    fields = attr.fields(struct)
    d = {f.name: f.metadata['label_key'] for f in fields}
    return d


def load_form_data():
    """ Load form data for requesting from json file"""
    request_form_data_fpath = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                           'request_form_data.json')
    form_data = json.loads(read_file(request_form_data_fpath))
    return form_data


def prepare_request_data(form_data: Dict, page_index: int) -> Dict:
    """ Just replace page index in existed form_data """
    _form_data = form_data

    # key that specify a index for current portion of data
    # it's kind of a convention that it goes first in request_form_data.json
    pagination_key = list(form_data.keys())[0]
    _form_data[pagination_key] = str(page_index)
    return _form_data


def gosreestr_parse_new_uids(fpath, existed_uids, timeout, error_timeout,  luigi_callback=None):
    page_index = 0
    s = requests.Session()
    headers = Headers(headers=True)

    _existed_uids = existed_uids

    if os.path.exists(fpath):
        parsed_uids = [u.split(';')[0] for u in read_lines(fpath)]
        page_index = int(read_lines(fpath).pop().split(';')[1]) + 1
        _existed_uids.extend(parsed_uids)

    form_data = prepare_request_data(FORM_DATA, page_index)
    s.headers = headers.generate()
    table_raw = s.post(LIST_URL, data=form_data, timeout=15).text
    status = ''
    new_uids_count = 0
    new_uids = list()
    while not check_empty_table(table_raw):
        uids = parse_ids_from_table(table_raw)
        _new_uids = list()
        for uid in uids:
            if uid not in _existed_uids:
                _new_uids.append(uid)
                append_file(fpath, f'{uid};{page_index}')
            else:
                break

        new_uids.extend(_new_uids)
        new_uids_count += len(_new_uids)

        form_data = prepare_request_data(FORM_DATA, page_index)

        try:
            s.headers = headers.generate()
            table_raw = s.post(LIST_URL, data=form_data, timeout=15).text
        except (ReadTimeout, ConnectTimeout, ConnectionError, ReadTimeoutError):
            luigi_callback(f'Page: {page_index}, parsed count: {new_uids_count}. Timeout after error', 0)
            sleep(error_timeout)
        else:
            page_index += 1
            luigi_callback(f'Page: {page_index}, parsed count: {new_uids_count}. Timeout after success.', 0)
            sleep(timeout)

    return new_uids


def gosreestr_parse_companies(fpath: str, struct=None):

    page_index = 23
    s = requests.Session()
    headers = Headers(headers=True)

    form_data = prepare_request_data(FORM_DATA, page_index)

    table_raw = s.post(LIST_URL, data=form_data).text
    mapping = {f.name: f.metadata['label_key'] for f in attr.fields(GosreestrCompany)}

    timeout_error = False

    while not check_empty_table(table_raw):
        ids = parse_ids_from_table(table_raw)
        if not timeout_error:
            for _id in ids:
                url = DETAIL_URL.format(_id)
                try:
                    s.headers = headers.generate()
                    company_raw = s.get(url, timeout=10).text
                except (ReadTimeout, ConnectTimeout, ConnectionError, ReadTimeoutError):
                    print('company request ban')
                    timeout_error = True
                    sleep(90)
                else:
                    timeout_error = False
                d = parse_company_info(company_raw, mapping)
                print(d)
                # sleep(15)
        page_index += 1
        form_data = prepare_request_data(FORM_DATA, page_index)
        sleep(300)
        try:
            s.headers = headers.generate()
            table_raw = s.post(LIST_URL, data=form_data, timeout=10).text
        except (ReadTimeout, ConnectTimeout, ConnectionError, ReadTimeoutError):
            print('table request ban')
            timeout_error = True
            sleep(300)
        else:
            timeout_error = False


gosreestr_parse_companies('c:\\Users\\elessar\\temp\\', GosreestrCompany)
