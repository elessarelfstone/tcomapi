import argparse
import os
import sys

import validators


def parse_args():
    """ Parse CLI arguments and return validated values """
    parser = argparse.ArgumentParser(
        description="Tool for retrieving taxes payments data of KZ companies",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("address_port", type=str,
                        help="target host and port")

    parser.add_argument("token", type=str,
                        help="secret token")

    parser.add_argument("fpath", type=validators.check_fpath,
                        help="input file with BINs")

    parser.add_argument("date_range", type=validators.check_date_range,
                        help="date range for which we retrieve data")

    parser.add_argument("-B", "--backoff", default=0.5,
                        type=validators.check_positive_float,
                        help="delay after each failed attempt")
    parser.add_argument("-w", "--timeout", default=4,
                        type=validators.check_positive_float,
                        help="server connect timeout")
    parser.add_argument("-r", "--retries",
                        default=4, type=validators.check_positive_float,
                        help="retries count for one BIN")
    parser.add_argument("-f", "--fsize",
                        default=50000000,type=validators.check_positive_int,
                        help="size limit for output file")

    return parser.parse_args()
