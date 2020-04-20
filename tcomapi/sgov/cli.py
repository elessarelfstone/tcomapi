import argparse

import tcomapi.common.cli_validators as validators


def parse_args():
    """ Parse CLI arguments and return validated values """
    parser = argparse.ArgumentParser(
        description="Tool for retrieving information on tax payments by Kazakhstan companies",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("fpath", type=validators.check_fpath,
                        help="input file with BINs")

    parser.add_argument("-rl", "--ratelimit", default=10,
                        type=validators.check_positive_int,
                        help="rate limit(parallelism limit)")

    parser.add_argument("-sl", "--semlimit",
                        default=20, type=validators.check_positive_int,
                        help="semaphore limit(request frequency limit)")

    parser.add_argument("-f", "--fsize",
                        default=100000000, type=validators.check_positive_int,
                        help="size bytes per output file")

    return parser.parse_args()
