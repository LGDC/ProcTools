"""Simple helper objects for miscellaneous needs.

Do not put anything here which imports from other ProcTools submodules!
"""
import datetime
import logging
import random
import types


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def datestamp(fmt="%Y_%m_%d"):
    """Return string with current datestamp.

    Args:
        fmt (str): String-formatting for stamp.

    Returns:
        str
    """
    return timestamp(fmt)


def elapsed(start_time, logger=None, log_level=logging.INFO):
    """Return time-delta since start time.

    Args:
        start_time (datetime.datetime): Start to measure time elapsed since.
        logger (logging.Logger, None): If not None, logger to emit elapsed message.
        log_level (int): Level to emit elapsed message at.

    Returns:
        datetime.timedelta
    """
    span = datetime.datetime.now() - start_time
    if logger:
        logger.log(
            log_level,
            "Elapsed: %s hrs, %s min, %s sec.",
            (span.days * 24 + span.seconds // 3600),
            ((span.seconds // 60) % 60),
            (span.seconds % 60)
        )
    return span


def parity(numbers):
    """Return proper parity description for a collection of numbers.

    Parity description can be: "even", "odd", or "mixed".

    Args:
        numbers (iter): Collection of numbers.

    Returns:
        str
    """
    numbers_bitwise = {n & 1 for n in numbers}
    if not numbers_bitwise:
        _parity = None
    elif len(numbers_bitwise) == 1:
        _parity = {0: "even", 1: "odd"}[numbers_bitwise.pop()]
    else:
        _parity = "mixed"
    return _parity


def randomized(iterable):
    """Generate sequence of items in random order.

    Args:
        iterable (iter): Collection of items to yield.

    Yields:
        object: Item from iterable.
    """
    if isinstance(iterable, types.GeneratorType):
        iterable = set(iterable)
    for item in random.sample(population=iterable, k=len(iterable)):
        yield item


def timestamp(fmt="%Y_%m_%d_T%H%M"):
    """Return string with current timestamp.

    Args:
        fmt (str): String-formatting for stamp.

    Returns:
        str
    """
    return datetime.datetime.now().strftime(fmt)
