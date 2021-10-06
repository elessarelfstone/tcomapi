from math import floor
from time import sleep

from box import Box
from requests import Session
from requests.exceptions import ConnectionError, ReadTimeout, ConnectTimeout
from requests.auth import HTTPBasicAuth


class InfobipApiParser:

    def __init__(self, url, entity, sleep_timeout,
                 user, password, params=None, header=None):

        self.session = Session()

        self.url = url
        self.params = params
        self.header = header
        self.sleep_timeout = sleep_timeout

        # basic auth
        self.user = user
        self.password = password

        self.entity = entity

        self._json = None
        self._total = 0
        self._curr_page = 0

        # counter for parsed items
        self._parsed_count = 0

    @property
    def status_message(self):
        return f'Total: {self._total}. Parsed {self._parsed_count}'

    @property
    def parsed_count(self):
        return self._parsed_count

    @property
    def percent_done(self):
        if self._total == 0:
            return 0
        else:
            return floor((self._parsed_count * 100) / self._total)

    @property
    def curr_page(self):
        return self._curr_page

    def next_page_params(self):

        params = Box(self.params)

        if self._json is None:
            return params.to_dict()

        b = Box(self._json)

        if b[self.entity]:
            pagination = b.pagination
            self._total = pagination.totalItems
            self._curr_page = pagination.page
            params.page = self._curr_page + 1

            if ((self._curr_page+1) * pagination.limit) >= self._total:
                params = Box({})

        else:
            params = Box({})

        return params.to_dict()

    def parse(self):
        return self._json[self.entity]

    def load(self, params):

        error_count = 0
        raw = None
        while raw is None:
            try:

                r = self.session.get(self.url, params=params,
                                     auth=HTTPBasicAuth(self.user, self.password))

                if r.status_code != 200:
                    r.raise_for_status()

                raw = r.json()

                if raw is None:
                    raise Exception('Result is None')

            except (ConnectionError, ReadTimeout, ConnectTimeout) as e:
                error_count += 1
                sleep(self.sleep_timeout * 2)
                if error_count > 10:
                    raise e

        return raw

    def __iter__(self):

        curr_page_params = self.next_page_params()

        while curr_page_params:
            self._json = self.load(curr_page_params)
            data = self.parse()
            self._parsed_count += len(data)
            yield data
            sleep(self.sleep_timeout)
            curr_page_params = self.next_page_params()
