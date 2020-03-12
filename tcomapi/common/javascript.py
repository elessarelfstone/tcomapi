import json
import codecs
import re

from bs4 import BeautifulSoup


from tcomapi.common.exceptions import ExternalSourceError
from tcomapi.common.utils import load_html


def load_js_raw(url):
    r = codecs.encode(load_html(url), encoding="utf8")
    soup = BeautifulSoup(r, 'lxml')
    scripts = soup.find_all('script')
    return ''.join([str(s) for s in scripts])


def parse_json_from_js(url, pattern):
    r = re.search(pattern, load_js_raw(url))
    if r:
        return json.loads(r.group(2))
    else:
        raise ExternalSourceError('Javascript data not found')
