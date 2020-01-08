"""Simple helper objects for miscellaneous needs.

Do not put anything here which imports from other ProcTools submodules!
"""
import datetime
import logging
import random
import socket
import types


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def access_odbc_string(database_path):
    """Return ODBC connection string to Microsoft Access database.

    Args:
        database_path (str): Path to Access database.

    Returns
        str
    """
    details = {
        "database": database_path,
        "driver": "Microsoft Access Driver (*.mdb, *.accdb)",
    }
    return "DRIVER={{{driver}}};DBQ={database}".format(**details)


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
            (span.seconds % 60),
        )
    return span


def last_date(day_name, **kwargs):
    """Return the last date that the given day occurred on.

    Args:
        day_name (str): Name of the day to find the last date for.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        reference_date (datetime.date, datetime.datetime): Date of reference to work
            back to the given day from. Default is current date.

    Returns:
        datetime.date
    """
    reference_date = kwargs.get("reference_date", datetime.date.today())
    days_of_week = [
        "sunday",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
    ]
    delta_day = days_of_week.index(day_name.lower()) - reference_date.isoweekday()
    if delta_day >= 0:
        delta_day -= 7
    day_date = reference_date + datetime.timedelta(days=delta_day)
    if isinstance(day_date, datetime.datetime):
        day_date = day_date.date()
    return day_date


def log_entity_states(entity_type, states, logger=None, **kwargs):
    """Log the counts for entities in each state from provided counter.

    Args:
        entity_type (str): Label for the entity type whose states are counted.
            Preferably plural, e.g. "datasets".
        states (collections.Counter): State-counts.
        logger (logging.Logger): Loger to handle emitted loglines.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        fmt (str): Format-string for logline. Use keywords in default value (`state` &
            `count` are the key & value of a single item in `states`). Default is
            "{count} {entity_type} {state}."
        log_level (int): Level to log the function at. Default is 20 (logging.INFO).
    """
    if not logger:
        logger = LOG
    level = kwargs.get("log_level", logging.INFO)
    if sum(states.values()) == 0:
        logger.log(level, "No %s states to log.", entity_type)
    else:
        for state, count in sorted(states.items()):
            line = kwargs.get("fmt", "{count} {entity_type} {state}.").format(
                count=count, entity_type=entity_type, state=state
            )
            logger.log(level, line)


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


def sql_server_odbc_string(
    host, database_name=None, username=None, password=None, **kwargs
):
    """Return ODBC connection string for SQL Server database.

    Defaults to trusted connection. If username and password are defined, they
    will override the trusted connection setting.
    Depending on your ODBC setup, omitting a login and trusted will either
    fail or prompt for credentials.

    Args:
        host (str): Name of the SQL Server instance host.
        database_name (str, None): Name of the database (optional).
        username (str): Username to connect with (optional).
        password (str): Password to connect with (optional).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        driver_string (str): ODBC string for driver & version. Default is "{ODBC Driver
            17 for SQL Server}".
        application (str): Name of application to represent connection as being from.
        read_only (bool): True if application intent is for read-only workload. Default
            is False.

    Returns:
        str

    """
    _string = "Driver={};Server={};".format(
        kwargs.get("driver_string", "{ODBC Driver 17 for SQL Server}"), host
    )
    if database_name:
        _string += "Database={};".format(database_name)
    if username:
        _string += "UID={};".format(username)
        if password:
            _string += "PWD={};".format(password)
    else:
        _string += "Trusted_Connection=yes;"
    if kwargs.get("application"):
        _string += "APP={};".format(kwargs["application"])
    if kwargs.get("read_only", False):
        _string += "ApplicationIntent=ReadOnly;"
    else:
        _string += "ApplicationIntent=ReadWrite;"
    _string += "WSID={}".format(socket.getfqdn())
    return _string


def timestamp(fmt="%Y_%m_%d_T%H%M"):
    """Return string with current timestamp.

    Args:
        fmt (str): String-formatting for stamp.

    Returns:
        str
    """
    return datetime.datetime.now().strftime(fmt)
