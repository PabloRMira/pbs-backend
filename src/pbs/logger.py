import functools
import logging
import sys
from typing import Callable

logger = logging.getLogger("pbs")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - [%(levelname)s] %(message)s")
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


def log_step(func: Callable):
    funcpath = f"{func.__module__}.{func.__name__}"

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"{funcpath} started...")
        return func(*args, **kwargs)

    return wrapper
