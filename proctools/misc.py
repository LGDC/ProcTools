"""Simple helper objects for miscellaneous needs.

Do not put anything here which imports from other ProcTools submodules!
"""
from collections import Counter
from datetime import date, datetime, timedelta
from logging import INFO, Logger, getLogger
from pathlib import Path
from random import sample
from socket import getfqdn
from types import GeneratorType
from typing import Any, Iterable, Iterator, Optional, Union


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""


def access_odbc_string(database_path: Union[Path, str]) -> str:
    """Return ODBC connection string to Microsoft Access database.

    Args:
        database_path: Path to Access database.
    """
    return f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={database_path}"


def time_elapsed(
    start_time: datetime,
    *,
    logger: Optional[Logger] = None,
    log_level: int = INFO,
) -> timedelta:
    """Return time-delta since start time.

    Args:
        start_time: Starting point to measure time elapsed since.
        logger: Logger to emit elapsed message.
        log_level: Level to log elapsed message at.
    """
    delta = datetime.now() - start_time
    if logger:
        logger.log(
            log_level,
            "Elapsed: %s hrs, %s min, %s sec.",
            (delta.days * 24 + delta.seconds // 3600),
            ((delta.seconds // 60) % 60),
            (delta.seconds % 60),
        )
    return delta


def last_date_of_day(
    day_name: str, *, date_of_reference: Optional[Union[date, datetime]] = None
) -> date:
    """Return the last date that the given day occurred on.

    Args:
        day_name: Name of the day to find the last date for.
        date_of_reference: Date of reference to work back to the given day from. If set
            to None, will work back from current date.
    """
    if date_of_reference is None:
        date_of_reference = date.today()
    days_of_week = [
        "sunday",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
    ]
    delta_day = days_of_week.index(day_name.lower()) - date_of_reference.isoweekday()
    if delta_day >= 0:
        delta_day -= 7
    day_date = date_of_reference + timedelta(days=delta_day)
    if isinstance(day_date, datetime):
        day_date = day_date.date()
    return day_date


def log_entity_states(
    entity_label: str,
    states: Counter,
    *,
    logger: Optional[Logger] = None,
    log_level: int = INFO,
    logline_format: str = "{count:,} {entity_type} {state}.",
) -> None:
    """Log the counts for entities in each state from provided counter.

    Args:
        entity_label: Label for the entity type whose states are counted. Preferably
            plural, e.g. "datasets".
        states: State-counts.
        logger: Logger to handle emitted loglines. If not specified, will use module
            -level logger.
        log_level: Level to log the function at.
        logline_format: Formating string for logline. Use keywords in default value
            (`state` & `count` are the key & value of a single item in the the Counter
            `states`).
    """
    if not logger:
        logger = LOG
    if sum(states.values()) == 0:
        logger.log(log_level, "No %s states to log.", entity_label)
    else:
        for state, count in sorted(states.items()):
            logger.log(
                log_level,
                logline_format.format(
                    count=count, entity_type=entity_label, state=state
                ),
            )


def merge_common_collections(*collections: Iterable[Any]) -> Iterator[set]:
    """Generate sets of merged non-mapping collections that share any items.

    Args:
        *collections: Collections to merge.
    """
    merged_collections = []
    merge_has_happened = False
    for collection in collections:
        collection = set(collection)
        for merged_collection in merged_collections:
            if not merged_collection.isdisjoint(collection):
                merged_collection |= collection
                merge_has_happened = True
                break

        else:
            merged_collections.append(collection)
    if merge_has_happened:
        merged_collections = merge_common_collections(*merged_collections)
    yield from merged_collections


def randomized(iterable: Iterable[Any]) -> Iterator[Any]:
    """Generate sequence of elements in random order.

    Args:
        iterable: Collection of elements to randomly generate from.
    """
    if isinstance(iterable, GeneratorType):
        iterable = set(iterable)
    yield from sample(population=iterable, k=len(iterable))


def sql_server_odbc_string(
    hostname: str,
    database_name: Optional[str] = None,
    *,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    application_name: Optional[str] = None,
    driver_string: str = "{ODBC Driver 17 for SQL Server}",
    read_only: bool = False,
) -> str:
    """Return ODBC connection string for SQL Server database.

    Notes:
        Defaults to trusted connection. If username and password are defined, they
            will override the trusted connection setting.
        Depending on your ODBC setup, omitting a login and trusted will either
            fail or prompt for credentials.

    Args:
        hostname: Host name of SQL Server instance.
        database_name: Name of database to begin session in.
        port: Port to connect to instance on.
        username: Name of user for authentication with instance.
        password: Password for authentication with instance.
        application_name: Name of application to represent connection as being from.
        driver_string: ODBC string for driver & version.
        read_only: Application intent is for read-only workload if True.
    """
    host = hostname if port is None else f"{hostname},{port}"
    odbc_string = f"Driver={driver_string};Server={host};"
    if database_name:
        odbc_string += f"Database={database_name};"
    if username:
        odbc_string += f"UID={username};"
        if password:
            odbc_string += f"PWD={password};"
    else:
        odbc_string += "Trusted_Connection=yes;"
    if application_name:
        odbc_string += f"APP={application_name};"
    if read_only:
        odbc_string += "ApplicationIntent=ReadOnly;"
    else:
        odbc_string += "ApplicationIntent=ReadWrite;"
    odbc_string += f"WSID={getfqdn()};"
    return odbc_string


def timestamp(fmt="%Y_%m_%d_T%H%M") -> str:
    """Return string with current timestamp.

    Args:
        fmt: String-formatting for stamp.
    """
    return datetime.now().strftime(fmt)
