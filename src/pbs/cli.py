import argparse
import functools
from typing import Callable


def make_cli(func: Callable):
    @functools.wraps(func)
    def wrapper():
        parser = argparse.ArgumentParser()
        for arg, argtype in func.__annotations__.items():
            arg_adj = f"--{arg.replace('_', '-')}"
            parser.add_argument(arg_adj, type=argtype)
        args = parser.parse_args().__dict__
        return func(**args)

    return wrapper
