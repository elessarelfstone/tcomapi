from tcomapi.common.exceptions import BadDataType


def is_float(instance, attribute, value):
    try:
        return float(value.replace(',', '.'))
    except ValueError:
        raise BadDataType('{} cannot be represented as float')


