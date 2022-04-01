"""Process manager objects."""
from datetime import datetime as _datetime
from functools import partial
from logging import INFO, FileHandler, Formatter, Logger, StreamHandler, getLogger
from operator import itemgetter
import os
from pathlib import Path
import sqlite3
from types import FunctionType
from typing import Dict

from jinja2 import Environment, PackageLoader

from proctools.communicate import extract_email_addresses, send_email_smtp
from proctools.filesystem import create_folder
from proctools.misc import elapsed
from proctools.value import datetime_from_string


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""

PROC_PATH: Path = (
    Path(os.environ["PROC_PATH"])
    if "PROC_PATH" in os.environ
    else Path(os.environ.get("LOCALAPPDATA"), "proc")
)
"""Path to folder for processing environment."""
LOGS_PATH: Path = PROC_PATH / "logs"
"""Path to folder for logging content."""
RUN_RESULTS_DB_PATH: Path = LOGS_PATH / "Run_Results.sqlite3"
"""Path to execution run-results database."""

RUN_STATUS_DESCRIPTION: Dict[int, str] = {1: "complete", 0: "failed", -1: "incomplete"}
"""Mapping of status number to description."""


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
            send_email_smtp(
                from_address=from_address,
                to_addresses=kwargs.get("to_addresses"),
                copy_addresses=kwargs.get("copy_addresses"),
                blind_copy_addresses=kwargs.get("blind_copy_addresses"),
                reply_to_addresses=kwargs.get("reply_to_addresses"),
                subject=kwargs["subject"],
                body=kwargs["body"],
                body_type="html",
                attachment_paths=kwargs.get("attachment_paths"),
                host=host,
                port=kwargs.get("port", 25),
                password=kwargs.get("password"),
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
            start_time = _datetime.now().isoformat(" ")
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
            end_time = None if value == -1 else _datetime.now().isoformat(" ")
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
        logger = getLogger()
        logger.setLevel(INFO)
        formatter = Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        # Need to remove old handlers, to avoid duplicating handlers between procedures.
        for handler in logger.handlers:
            logger.removeHandler(handler)
        console_handler = StreamHandler()
        console_handler.setLevel(INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        create_folder(LOGS_PATH, create_parents=True, exist_ok=True)
        file_handler = FileHandler(
            filename=LOGS_PATH / (member_name + ".log"), mode=file_mode
        )
        file_handler.setLevel(file_level if file_level else INFO)
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
            start_time = _datetime.now()
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
