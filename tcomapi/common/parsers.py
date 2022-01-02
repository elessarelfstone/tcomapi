import abc
from abc import ABC, abstractmethod
from math import floor
from time import sleep


from box import Box
from requests import Session
from requests.exceptions import ConnectionError, ReadTimeout, ConnectTimeout
from requests.auth import HTTPBasicAuth


class AuthSession:

    def __init__(self, parser):
        self.parser = parser
        self.session = Session()

    def get(self, url, params, headers):
        if hasattr(self.parser, 'user'):
            auth = HTTPBasicAuth(self.parser.user, self.parser.password)
            return self.session.get(url, params=params, headers=headers,
                                    auth=auth, verify=False)
        else:
            return self.session.get(url, params=params,
                                    headers=headers, verify=False)


class BaseApiParser(ABC):

    def __init__(self, url, entity, sleep_timeout,
                 params=None, headers=None):

        self.session = Session()

        self.url = url
        self.params = params
        self.headers = headers
        self.sleep_timeout = sleep_timeout

        self.entity = entity

        self._box = None
        self._total = 0
        self._curr_page = 0

        # counter for parsed items
        self._parsed_count = 0

    @property
    @abstractmethod
    def total(self):
        pass

    @property
    def parsed_count(self):
        return self._parsed_count

    @property
    def percent_done(self):
        if not self.total:
            return 0

        return floor((self._parsed_count * 100) / self.total)

    @property
    def status_message(self):
        return f'Total: {self.total}. Parsed items: {self.parsed_count}'

    @property
    @abstractmethod
    def page(self):
        pass

    @property
    @abstractmethod
    def size(self):
        pass

    @property
    def curr_page(self):
        return self._curr_page

    def parse(self):
        if not self._box:
            return None

        return self._box.to_dict()[self.entity]

    def next_page_params(self):

        params = Box(self.params)

        # first getting params
        if self._box is None:
            return params.to_dict()

        # check if there are items
        if self._box[self.entity]:
            params.page = self.page + 1

            # prevent next request if will be no items
            if ((self.page + 1) * self.size) >= self.total:
                params = Box({})
        else:
            params = Box({})

        return params.to_dict()

    @abstractmethod
    def load(self, params):
        pass

    def __iter__(self):

        curr_page_params = self.next_page_params()

        while curr_page_params:
            self._box = Box(self.load(curr_page_params))
            data = self.parse()
            self._parsed_count += len(data)
            yield data
            sleep(self.sleep_timeout)
            curr_page_params = self.next_page_params()
