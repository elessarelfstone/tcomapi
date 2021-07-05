import csv
import os
import fnmatch
from datetime import datetime, timedelta
from os.path import basename, exists, join
from collections import deque
from time import sleep

import attr
import luigi
import requests
from requests import ConnectionError, HTTPError
from luigi.contrib.ftp import RemoteTarget, RemoteFileSystem
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session

from tasks.base import BigDataToCsv
from tcomapi.common.utils import build_fpath, get, read_lines, append_file
from settings import (TELECOM_API_TOKEN_URI, TELECOM_API_HOST,
                      TELECOM_API_HEADERS,  TELECOM_API_TOKEN_URL,
                      TELECOM_API_CLIENT_ID, TELECOM_API_CLIENT_SECRET,
                      TELECOM_API_USER_NAME, TELECOM_API_PASSWORD,
                      FTP_HOST, FTP_PATH, FTP_PASS, FTP_USER, FTP_IN_PATH,
                      TEST_DIR, BIGDATA_TMP_DIR, TELECOM_API_URL_PATTERN, TMP_DIR)


def read_tsv(tsv_fpath: str):
    rows = []
    with open(tsv_fpath) as f:
        rd = csv.reader(f, delimiter="\t", quotechar='"')
        for row in rd:
            rows.append(tuple(row))

    return rows


class NoIINsToVerify(Exception):
    pass


@attr.s
class ClientInfo:
    customer_id = attr.ib(default='')
    local_abonent_id = attr.ib(default='')
    filial_id = attr.ib(default='')
    iin = attr.ib(default='')
    enriched_mobile_phone = attr.ib(default='')
    status = attr.ib(default='')


class TokenManager:

    def __init__(self):
        self.expires_at = datetime.now() - timedelta(seconds=15)
        self.set_token()

    def set_token(self):
        # if datetime.now() > self.expires_at:
        _token = self.load_token()
        self.access_token = _token['access_token']
        self.refresh_token = _token['refresh_token']
        self.expires_in = _token['expires_in']
        self.expires_at = datetime.fromtimestamp(_token['expires_at']) - timedelta(seconds=15)

    @property
    def token(self):
        if datetime.now() > self.expires_at:
            self.set_token()

        return self.access_token

    @staticmethod
    def load_token():
        oauth = OAuth2Session(client=LegacyApplicationClient(client_id=TELECOM_API_CLIENT_ID))
        token = oauth.fetch_token(token_url=TELECOM_API_TOKEN_URL, username=TELECOM_API_USER_NAME,
                                  password=TELECOM_API_PASSWORD, client_id=TELECOM_API_CLIENT_ID,
                                  client_secret=TELECOM_API_CLIENT_SECRET)

        return token


class InputIins(luigi.ExternalTask):

    """
    """

    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(build_fpath(TEST_DIR, self.name, 'csv'))


class BmgIINVerification(BigDataToCsv):

    name = luigi.Parameter(default='bmg_verif')

    def get_data_to_parse(self):

        parsed_iins = []
        if os.path.exists(self.parsed_fpath):
            parsed_iins = [tuple(u.strip().split(',')) for u in read_lines(self.parsed_fpath)]

        source_data = read_tsv(self.input().path)

        _data = []
        if parsed_iins:
            for d in source_data:
                _d = (d[-2], d[-1])
                if _d in parsed_iins:
                    _data.append(d)
        else:
            _data = source_data

        return _data, len(source_data), len(parsed_iins)

    def load(self, iin, phone, headers):
        url = TELECOM_API_URL_PATTERN.format(iin, f'7{phone}')
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return '1' if r.json()['data']['verification_state'] else '0'

        else:
            r.raise_for_status()

    def requires(self):
        return InputIins(name=self.name)

    def output(self):
        return luigi.LocalTarget(build_fpath(BIGDATA_TMP_DIR, self.name, '.csv'))

    def run(self):
        token_m = TokenManager()

        headers = TELECOM_API_HEADERS
        iins, all_cnt, parsed_cnt = self.get_data_to_parse()
        iins = deque(iins)

        while iins:
            try:
                print('adasdsad')
                headers['Authorization'] = 'Bearer {}'.format(token_m.token)
                curr = ClientInfo(*iins.popleft())
                url = TELECOM_API_URL_PATTERN.format(curr.iin, f'7{curr.enriched_mobile_phone}')
                r = requests.get(url, headers=headers)
                if r.status_code == 200:
                    curr.status = '1' if r.json()['data']['verification_state'] else '0'
                elif r.status_code == 401:
                    token_m.set_token()
                    r.raise_for_status()

            except HTTPError as e:
                sleep(10)
                print('http error')

            except ConnectionError:
                print('conn error')
                iins.appendleft(attr.astuple(curr))
                sleep(60*60*4)
                token_m.set_token()
            else:
                print('success')
                row = attr.astuple(curr)
                append_file(self.output().path, ','.join(row))
                parsed_cnt += 1
                append_file(self.parsed_fpath, ','.join([curr.iin, curr.enriched_mobile_phone]))
                percent = round((100*len(iins))/all_cnt)
                self.set_status(f'All iins count: {all_cnt}. Parsed: {parsed_cnt}', percent)
                sleep(6)

        append_file(self.success_fpath, 'ok')


class RunBmgVerif(luigi.WrapperTask):
    def requires(self):
        yield BmgIINVerification(directory=TMP_DIR)


if __name__ == '__main__':
    luigi.run()






