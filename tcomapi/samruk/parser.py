from math import floor
from time import sleep

from box import Box
from requests.exceptions import ConnectionError, ReadTimeout, ConnectTimeout

from tcomapi.common.parsers import BaseApiParser, AuthSession


class SamrukApiParser(BaseApiParser):

    def __init__(self, url, entity, sleep_timeout,
                 user, password,
                 params=None, headers=None):

        super().__init__(url, entity, sleep_timeout,
                         params=params, headers=headers)

        # basic auth
        self.user = user
        self.password = password
        self.session = AuthSession(self)

    @property
    def total(self):
        return self._box.totalElements

    @property
    def page(self):
        return self._box.page

    @property
    def size(self):
        return self._box.size

    def load(self, params):

        error_count = 0
        raw = None
        while raw is None:
            try:
                r = self.session.get(self.url, params, self.headers)

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


class SamrukPlansApiParser(SamrukApiParser):

    def __init__(self, url, entity, sleep_timeout,
                 user, password,
                 params=None, headers=None):

        super().__init__(url, entity, sleep_timeout,
                         user, password,
                         params=params, headers=headers)

    @property
    def page(self):
        return Box(self._box).number
