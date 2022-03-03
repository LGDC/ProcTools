"""Metadata objects."""
from dataclasses import asdict, dataclass, field
import datetime
from functools import partial
from itertools import chain
import logging
from operator import itemgetter
import os
from pathlib import Path
import sqlite3
from types import FunctionType
from typing import Any, Optional
from urllib.parse import quote_plus

from jinja2 import Environment, PackageLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import arcproc

from .communicate import (  # pylint: disable=relative-beyond-top-level
    extract_email_addresses,
    send_email_smtp,
)
from .filesystem import create_folder  # pylint: disable=relative-beyond-top-level
from .misc import (  # pylint: disable=relative-beyond-top-level
    elapsed,
    sql_server_odbc_string,
)
from .value import datetime_from_string  # pylint: disable=relative-beyond-top-level


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

if "PROC_PATH" in os.environ:
    PROC_PATH = Path(os.environ["PROC_PATH"])
    """pathlib.Path: Path to folder for processing environment."""
else:
    PROC_PATH = Path(os.environ.get("LOCALAPPDATA"), "proc")
LOGS_PATH = PROC_PATH / "logs"
"""pathlib.Path: Path to folder for logging content."""
RUN_RESULTS_DB_PATH = LOGS_PATH / "Run_Results.sqlite3"
"""pathlib.Path: Path for execution run-result database."""
RUN_STATUS_DESCRIPTION = {1: "complete", 0: "failed", -1: "incomplete"}
"""dict: Mapping of status code to description."""


class Batch:
    """Representation of a batch of processing jobs.

    A batch is a group of jobs, generally related by their shared scheduling.

    Attributes:
        name (str): Name of the batch.
    """

    def __init__(self, name):
        """Initialize instance.

        Args:
            name (str): Name of the batch.
        """
        self.name = name
        self._conn = sqlite3.connect(RUN_RESULTS_DB_PATH)

    @property
    def id(self):  # pylint: disable=invalid-name
        """int: ID for batch, as found in Batch table."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "SELECT id FROM Batch WHERE name = ?;"
            cursor.execute(sql, [self.name])
            return cursor.fetchone()[0]

    @property
    def job_names(self):
        """list of str: Names of the jobs assigned to batch."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "SELECT name FROM Job WHERE batch_id = ?;"
            cursor.execute(sql, [self.id])
            return [name for name, in cursor.fetchall()]

    @property
    def last_job_run_metas(self):
        """list of dict: Metadata dictionaries for last job-runs."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "SELECT * FROM Last_Job_Run WHERE batch_id = ?;"
            cursor.execute(sql, [self.id])
            metas = [
                {column[0]: value for column, value in zip(cursor.description, row)}
                for row in cursor
            ]
            # Coerce timestamps to datetime--no sqlite3 date/time types, using text.
            for run_meta in metas:
                for key in ["start_time", "end_time"]:
                    run_meta[key] = datetime_from_string(run_meta[key])
        return metas

    @property
    def notification_addresses(self):
        """dict: Mapping of type to list of email addresses for notification."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = """
                SELECT
                    notification_to_addresses AS 'to_addresses',
                    notification_copy_addresses AS 'copy_addresses',
                    notification_blind_copy_addresses AS 'blind_copy_addresses',
                    notification_reply_to_addresses AS 'reply_to_addresses'
                FROM Batch
                WHERE name = ?
                LIMIT 1;
            """
            row = cursor.execute(sql, [self.name]).fetchone()
            if not row:
                raise ValueError("Batch name not valid member of Batch table.")

            addresses = {
                column[0]: list(extract_email_addresses(value))
                for column, value in zip(cursor.description, row)
            }
            return addresses

    @property
    def start_times(self):
        """set of tuples: Collection of tuples containing all last start times."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "SELECT start_time FROM Last_Job_Run WHERE batch_id = ?;"
            cursor.execute(sql, [self.id])
            times = {datetime_from_string(start_time) for start_time, in cursor}
            if None in times:
                times.remove(None)
        return times

    @property
    def status(self):
        """int: status ID for current batch run."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "SELECT status FROM Last_Job_Run WHERE batch_id = ?;"
            cursor.execute(sql, [self.id])
            return 1 if all(row[0] == 1 for row in cursor) else -1

    @property
    def status_description(self):
        """str: status description for current batch run."""
        return RUN_STATUS_DESCRIPTION[self.status]

    def send_notification(self, host, from_address, **kwargs):
        """Send email notification for batch.

        Args:
            host (str, None): Host name of SMTP server.
            from_address (str): Email address for sender.
            **kwargs: Arbitrary keyword arguments. See below.

        Kwargs:
            See keyword arguments for `proctools.communicate.send_email_smtp`.
        """
        if any(
            addresses
            for key, addresses in self.notification_addresses.items()
            if key in ["to_addresses", "copy_addresses", "blind_copy_addresses"]
        ):
            env = Environment(loader=PackageLoader("proctools", "templates"))
            template = env.get_template("batch_notification.html")
            last_run_metas = sorted(
                self.last_job_run_metas,
                key=itemgetter("start_time", "end_time"),
                reverse=True,
            )
            kwargs.update(self.notification_addresses)
            kwargs["subject"] = "Processing Batch: {} ({})".format(
                self.name, self.status_description
            )
            kwargs["body"] = template.render(last_run_metas=last_run_metas)
            send_email_smtp(host, from_address, body_type="html", **kwargs)


class Database:
    """Representation of database information.

    Attributes:
        data_schema_names (set of str): Collection of data schema names.
        host (str): Name & port configuration of the instance host.
        name (str): Name of the database.
    """

    def __init__(self, name, host, **kwargs):
        """Initialize instance.

        Args:
            name (str): Name of the database.
            host (str): Name of the SQL Server instance host. If indicating a port, add
                to host name after a comma.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            data_schema_names (iter of str): Collection of data schema names. Often used
                to identify which owned schemas need compressing. Default is empty set.
        """
        self.name = name
        self.host = host
        self.data_schema_names = set(kwargs.get("data_schema_names", set()))
        self._sqlalchemy = {}

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name!r}, host={self.host!r})"

    @property
    def hostname(self):
        """str: Name of the instance host."""
        return self.host.split(",")[0]

    def create_session(self, username=None, password=None, **kwargs):
        """Return SQLAlchemy session instance to database.

        Args:
            username (str): Name of user for credential (optional).
            password (str): Password for credential (optional).
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            See keyword args listed for `sql_server_odbc_string` function.

        Returns:
            sqlalchemy.orm.session.Session
        """
        odbc_string = self.get_odbc_string(username, password, **kwargs)
        url = self._sqlalchemy.setdefault(
            "url", f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_string)}"
        )
        engine = self._sqlalchemy.setdefault("engine", create_engine(url))
        return self._sqlalchemy.setdefault(
            "SessionFactory", sessionmaker(bind=engine)
        )()

    def get_odbc_string(self, username=None, password=None, **kwargs):
        """Return String necessary for ODBC connection.

        Args:
            username (str): Name of user for credential (optional).
            password (str): Password for credential (optional).

        Keyword Args:
            See keyword args listed for `sql_server_odbc_string` function.

        Returns:
            str
        """
        return sql_server_odbc_string(
            self.host, self.name, username, password, **kwargs
        )


@dataclass
class Field:
    """Representation of field information."""

    name: str
    """Name of the field."""
    type: str = "String"
    """Field type (case insensitve). See valid_types property for possible values. """
    length: int = 32
    "String field length."
    precision: int = 0
    "Single- or double-float field precision."
    scale: int = 0
    "Single- or double-float field scale."
    is_nullable: bool = True
    """Field is nullable if True."""
    is_required: bool = False
    """Field value is required if True."""
    alias: Optional[str] = None
    "Optional alias name of the field."
    default_value: Any = None
    """Default value for field on new feature."""

    is_id: bool = False
    """Field is part of the feature identifier if True."""
    not_in_source: bool = False
    """Field does not reside in source dataset(s) if True."""
    source_only: bool = False
    """Field resides on the source dataset(s) only if True."""


@dataclass
class Dataset2:
    """Representation of dataset information."""

    fields: "list[Field]" = field(default_factory=list)
    """List of field information objects."""
    geometry_type: Optional[str] = None
    """Type of geometry. NoneType indicates nonspatial."""
    path: Optional[Path] = None
    """Path to dataset."""
    source_path: Optional[Path] = None
    "Path to source dataset."
    source_paths: Optional["list[Path]"] = field(default_factory=list)
    "Paths to source dataset."

    @property
    def field_names(self) -> "list[str]":
        """Dataset field names."""
        return [field.name for field in self.fields]

    @property
    def id_field(self) -> Field:
        """Dataset identifier field. Will be NoneType if no single ID field."""
        return self.id_fields[0] if len(self.id_fields) == 1 else None

    @property
    def id_field_name(self) -> str:
        """Dataset identifier field names. Will be NoneType if no single ID field."""
        return self.id_field_names[0] if len(self.id_field_names) == 1 else None

    @property
    def id_field_names(self) -> "list[str]":
        """Dataset identifier field names."""
        return [field.name for field in self.id_fields]

    @property
    def id_fields(self) -> "list[Field]":
        """Dataset identifier field information objects."""
        return [field for field in self.fields if field.is_id]

    @property
    def out_field_names(self) -> "list[str]":
        """Output dataset field names."""
        return [field.name for field in self.out_fields]

    @property
    def out_fields(self) -> "list[Field]":
        """Output dataset field information objects."""
        return [field for field in self.fields if not field.source_only]

    @property
    def source_field_names(self) -> "list[str]":
        """Source dataset field names."""
        return [field.name for field in self.source_fields]

    @property
    def source_fields(self) -> "list[Field]":
        """Source dataset field information objects."""
        return [
            field
            for field in self.fields
            if field.source_only or not field.not_in_source
        ]

    def __fspath__(self) -> str:
        return str(self.path)

    def create(
        self,
        create_source: bool = False,
        override_path: Optional[Path] = None,
        spatial_reference_wkid: Optional[int] = None,
    ) -> Path:
        """Create dataset.

        Args:
            create_source: Create source version if True.
            override_path: Path to use instead of assoicated path.
            spatial_reference_wkid: Well-known ID for the spatial reference to apply. If
                None, dataset created will be nonspatial.
        """
        dataset_path = self.source_path if create_source else self.path
        dataset_path = override_path if override_path else dataset_path
        field_metadata_list = [
            asdict(field)
            for field in self.fields
            if (create_source and not field.not_in_source)
            or (not create_source and not field.source_only)
        ]
        return arcproc.dataset.create(
            dataset_path,
            field_metadata_list=field_metadata_list,
            geometry_type=self.geometry_type,
            spatial_reference_item=spatial_reference_wkid,
        )


class Job:
    """Representation of pipeline processing job.

    A job is an named & ordered sequence of processes to execute in a pipeline.

    Attributes:
        name (str): Name of the job.
        procedures (list): Ordered collection of procedures attached to job.
        run_id (int): ID value from Job_Run table in exec-results database. If
            job run has not yet been initiated, value is None.
    """

    def __init__(self, name, procedures=None):
        """Initialize instance.

        Args:
            name (str): Name of the job.
            procedures (iter, None): Collection of procedures to attach to job. If None,
                `self.procedures` will init as empty list.
        """
        self.name = name
        self.procedures = list(procedures) if procedures else []
        self.run_id = None
        self._conn = sqlite3.connect(RUN_RESULTS_DB_PATH)
        LOG.info("Initialized job instance for `%s`.", self.name)

    @property
    def id(self):  # pylint: disable=invalid-name
        """int: ID for job, as found in Job table."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "SELECT id FROM Job WHERE name = ?;"
            cursor.execute(sql, [self.name])
            return cursor.fetchone()[0]

    @property
    def run_status(self):
        """int: Run status for job-run, as found in Job_Run table.

        See RUN_STATUS_DESCRIPTION for valid status codes.
        """
        if self.run_id is None:
            return None

        with self._conn:
            cursor = self._conn.cursor()
            sql = "SELECT status FROM Job_Run WHERE id = ?;"
            cursor.execute(sql, [self.run_id])
            return cursor.fetchone()[0]

    @run_status.setter
    def run_status(self, value):
        if value not in RUN_STATUS_DESCRIPTION:
            raise ValueError(f"{value} not a valid status code")

        if self.run_id is None:
            start_time = datetime.datetime.now().isoformat(" ")
            with self._conn:
                sql = """
                    INSERT INTO Job_Run(status, job_id, start_time) VALUES (?, ?, ?);
                """
                self._conn.execute(sql, [value, self.id, start_time])
            with self._conn:
                cursor = self._conn.cursor()
                sql = "SELECT id FROM Job_Run WHERE job_id = ? AND start_time = ?;"
                cursor.execute(sql, [self.id, start_time])
                self.run_id = cursor.fetchone()[0]
        else:
            end_time = None if value == -1 else datetime.datetime.now().isoformat(" ")
            with self._conn:
                sql = """
                    UPDATE Job_Run SET status = ?, end_time = ? WHERE id = ?;
                """
                self._conn.execute(sql, [value, end_time, self.run_id])


class Pipeline:
    """Representation of an processing pipeline.

    Attributes:
        members (tuple): Ordered collection of pipeline execution members.
    """

    @staticmethod
    def init_logger(member_name, file_mode="a", file_level=None):
        """Initialize logger.

        Args:
            member_name (str): Name of pipeline member.
            file_mode (str): File mode to write logfile in.
            file_level (int, None): Log level above which to log to file.
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        # Need to remove old handlers, to avoid duplicating handlers between procedures.
        for handler in logger.handlers:
            logger.removeHandler(handler)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        create_folder(LOGS_PATH, create_parents=True, exist_ok=True)
        file_handler = logging.FileHandler(
            filename=LOGS_PATH / (member_name + ".log"), mode=file_mode
        )
        file_handler.setLevel(file_level if file_level else logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def __init__(self, *members):
        """Initialize instance.

        Args:
            *members: Ordered collection of pipeline execution members.
        """
        self.members = members

    def execute(self):
        """Execute pipeline members."""
        for member in self.members:
            start_time = datetime.datetime.now()
            if isinstance(member, Job):
                meta = {
                    "name": member.name,
                    "type": "job",
                    "procedures": member.procedures,
                }
                member.run_status = -1
            # Functions are assumed to be non-job standalone pipeline members.
            elif isinstance(member, (FunctionType, partial)):
                meta = {
                    "name": getattr(member, "__name__", "Unnamed Procedure"),
                    "type": "procedure",
                    "procedures": [member],
                }
            else:
                raise ValueError("Invalid pipeline member type")

            log = self.init_logger(meta["name"], file_mode="w", file_level=10)
            log.info("Starting %s: %s.", meta["type"], meta["name"])
            for procedure in meta["procedures"]:
                try:
                    procedure()
                except Exception:
                    log.exception("Unhandled exception")
                    raise

            meta["status"] = 1
            if meta["type"] == "job":
                member.run_status = meta["status"]
            elapsed(start_time, logger=log)
            log.info("%s %s.", meta["name"], RUN_STATUS_DESCRIPTION[meta["status"]])


def dataset_last_change_date(
    dataset_path, init_date_field_name="init_date", mod_date_field_name="mod_date"
):
    """Return date of the last change on dataset with tracking fields.

    Args:
        dataset_path (pathlib.Path, str): Path of the dataset.
        init_date_field_name (str): Name of the initial edit/create date field.
        mod_date_field_name (str): Name of the last edit/modification date field.

    Returns:

    """
    date_iters = arcproc.features.as_tuples(
        dataset_path, field_names=[init_date_field_name, mod_date_field_name]
    )
    dates = set(chain.from_iterable(date_iters))
    # datetimes cannot compare to NoneTypes.
    if None in dates:
        dates.remove(None)
    return max(dates) if dates else None
