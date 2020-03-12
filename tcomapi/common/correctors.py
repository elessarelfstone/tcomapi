from tcomapi.common.constants import CSV_SEP


def basic_corrector(value):
    return sep_clean(value).rstrip().replace('"', "'").replace('\n', '')


def sep_clean(value):
    return value.replace(CSV_SEP, '')


def date_corrector(value):
    # return common_corrector(value).split('+')[0]
    return value.split('+')[0]


def num_corrector(value):
    return sep_clean(basic_corrector(value))


def common_corrector(value):
    return '' if value is None else value


def bool_corrector(value):
    return str(value) if isinstance(value, bool) else value
