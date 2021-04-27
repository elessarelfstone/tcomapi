from codecs import encode
from typing import Dict, List


from bs4 import BeautifulSoup as Bs

from tcomapi.common.utils import read_file


class GosreestrParsingException(Exception):
    pass


def parse_company_info(raw: str, mapping: Dict) -> Dict:
    """ Parse html with detail company info.
        All values we need wrapped by <label> tag with 'for'
        attribute.
    """
    soup = Bs(encode(raw, encoding='utf-8'), 'lxml')
    info = dict()
    for col, key in mapping.items():
        el = soup.find('label', {'for': key})
        val = el.text if el else ''
        info[col] = val

    return info


def parse_ids_from_table(raw: str) -> List:
    """
    Parse identificators from <table> that contains list of companies.
    :param raw: Just a html with table.
    :return: List of identificators
    """

    def parse_id_from_url(url: str) -> int:
        return int(url.split('/')[-1])

    soup = Bs(encode(raw, encoding='utf-8'), 'lxml')
    links = soup.find_all('a', class_='btn-object-view')
    ids = [parse_id_from_url(link.get('href')) for link in links]
    return ids


def check_empty_table(raw: str) -> bool:
    soup = Bs(encode(raw, encoding='utf-8'), 'lxml')
    table = soup.find('table')
    is_empty = False
    if table:
        rows = table.find_all('tr')
        is_empty = False if len(rows) > 0 else True
    else:
        raise GosreestrParsingException('No data to parse')

    return is_empty
