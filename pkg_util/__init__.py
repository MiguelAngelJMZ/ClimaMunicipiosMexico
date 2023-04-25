import sys


def get_arg(arg: str, default: str = ""):
    try:
        argposition = sys.argv.index(arg)
        ret = sys.argv[argposition + 1]
    except Exception:
        ret = default

    return ret