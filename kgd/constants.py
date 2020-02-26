from enum import IntEnum

HOST = "data.egov.kz"


class ExitStatus(IntEnum):
    """Program exit code constants."""
    SUCCESS = 0
    ERROR = 1
    ERROR_NO_BINS = 2
    ERROR_TOO_MANY_FAILS = 3
    ERROR_SERVER_IS_DOWN = 4

    # 128+2 SIGINT <http://www.tldp.org/LDP/abs/html/exitcodes.html>
    ERROR_CTRL_C = 130


PROCESSED_NOT_EXISTS_MESSAGE = 'There are no processed BINs.'+'\n' +\
                               'Loading BINs to start retrieving data ... '
PROCESSED_EXISTS_MESSAGE = 'There are processed BINs.'+'\n' +\
                           'Loading BINs to continue retrieving data ...'

NO_BINS = 'No BINs to parse'
TOO_MANY_FAILS = 'Too many fails. Exiting...'
SRV_IS_DOWN = 'Address is unreachable. Exiting...'
SERVER_IS_DOWN = 'Address is unreachable. Exiting...'
