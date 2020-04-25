from tcomapi.common.constants import CSV_SEP, CSV_SEP_REPLACE


def clean_for_csv(value: str):
    # replace CSV_SEP symbol in value by ' '
    # unless we will have more columns than it needs to be
    _value = value.replace(CSV_SEP, CSV_SEP_REPLACE)

    # remove trailing newline
    # replace double quote by single qoute
    _value = _value.strip().replace('"', "'")

    return _value


def basic_corrector(value):
    return sep_clean(value).rstrip().replace('"', "'").replace('\n', '')


def sep_clean(value):
    return value.replace(CSV_SEP, '')


def date_corrector(value):
    return value.split('+')[0]


def num_corrector(value):
    return sep_clean(basic_corrector(value))


def common_corrector(value):
    return '' if value is None else value


def bool_corrector(value):
    return str(value) if isinstance(value, bool) else value


def float_corrector(value):
    return value.replace(',', '.')
