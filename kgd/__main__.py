import sys
import kgd.constants as cnst


def main():
    try:
        from kgd.core import main
        exit_status = main()
        if exit_status == 2:
            print(cnst.NO_BINS)
        elif exit_status == 3:
            print(cnst.TOO_MANY_FAILS)
        elif exit_status == 4:
            print(cnst.SRV_IS_DOWN)
    except KeyboardInterrupt:
        from kgd.constants import ExitStatus
        exit_status = ExitStatus.ERROR_CTRL_C

    sys.exit(exit_status)


if __name__ == "__main__":
    main()
