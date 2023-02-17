import sys
from colorama import init as colorama_init
from colorama import Fore
from colorama import Style

colorama_init()


def eprint(*args, **kwargs):
    print(Fore.RED, file=sys.stderr)
    print(*args, file=sys.stderr, **kwargs)
    print(Style.RESET_ALL, file=sys.stderr)


def error(*args, **kwargs):
    eprint(*args, **kwargs)
    raise Exception(*args)
