"""Process manager objects."""
from argparse import ArgumentParser
from datetime import datetime as _datetime
from logging import (
    DEBUG,
    INFO,
    FileHandler,
    Formatter,
    Logger,
    StreamHandler,
    getLogger,
)
from operator import itemgetter
from os import environ
from pathlib import Path
from sqlite3 import connect
from types import FunctionType
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Union

from jinja2 import Environment, PackageLoader

from proctools.communicate import extract_email_addresses, send_email_smtp
from proctools.misc import time_elapsed
from proctools.value import datetime_from_string


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""

PROC_PATH: Path = (
    Path(environ["PROC_PATH"])
    if "PROC_PATH" in environ
    else Path(environ.get("LOCALAPPDATA"), "ProcTools")
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
    """

    batch_id: int
    """ID for batch, as found in Batch table of the run results database."""
    name: str
    """Name of the batch."""

    def __init__(self, name: str) -> None:
        """Initialize instance.

        Args:
            name: Name of the batch.
        """
        self.name = name
        self._conn = connect(RUN_RESULTS_DB_PATH)

        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute("SELECT id FROM Batch WHERE name = ?;", [self.name])
            self.batch_id = cursor.fetchone()[0]

    @property
    def job_names(self) -> List[str]:
        """Names of jobs in the batch."""
        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute("SELECT name FROM Job WHERE batch_id = ?;", [self.batch_id])
            return [name for name, in cursor.fetchall()]

    @property
    def job_last_run_records(self) -> List[Dict[str, Union[_datetime, int, str]]]:
        """List of dictionaries for last run records for jobs in the batch."""
        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT * FROM Last_Job_Run WHERE batch_id = ?;", [self.batch_id]
            )
            records = [
                {column[0]: value for column, value in zip(cursor.description, row)}
                for row in cursor
            ]
            # Coerce timestamps to datetime--no sqlite3 date/time types, using text.
            for record in records:
                for key in ["start_time", "end_time"]:
                    record[key] = datetime_from_string(record[key])
        return records

    @property
    def job_last_run_start_times(self) -> Set[_datetime]:
        """Set of last-run start times for jobs in the batch."""
        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT start_time FROM Last_Job_Run WHERE batch_id = ?;",
                [self.batch_id],
            )
            times = {datetime_from_string(start_time) for start_time, in cursor}
            if None in times:
                times.remove(None)
        return times

    @property
    def notification_addresses(self) -> Dict[str, List[str]]:
        """Mapping of type to list of email addresses for notification."""
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
    def status(self) -> int:
        """Status code for current batch run."""
        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT status FROM Last_Job_Run WHERE batch_id = ?;", [self.batch_id]
            )
            return 1 if all(status == 1 for status, in cursor) else -1

    @property
    def status_description(self) -> str:
        """Status description for current batch run."""
        return RUN_STATUS_DESCRIPTION[self.status]

    def send_notification(
        self,
        *,
        from_address: str,
        host: str,
        port: int = 25,
        password: Optional[str] = None,
    ) -> None:
        """Send email notification for batch.

        Args:
            from_address: Email address for sender.
            host: Host name of SMTP server.
            port: Port to connect to SMTP host on.
            password: Password for authentication with host.
        """
        if not any(
            addresses
            for key, addresses in self.notification_addresses.items()
            if key in ["to_addresses", "copy_addresses", "blind_copy_addresses"]
        ):
            LOG.info("No recipients for notification; not sending.")
            return

        env = Environment(loader=PackageLoader("proctools", "templates"))
        template = env.get_template("batch_notification.html")
        records = sorted(
            self.job_last_run_records,
            key=itemgetter("start_time", "end_time"),
            reverse=True,
        )
        send_email_smtp(
            from_address=from_address,
            **self.notification_addresses,
            subject=f"Processing Batch: {self.name} ({self.status_description})",
            body=template.render(job_last_run_records=records),
            body_type="html",
            host=host,
            port=port,
            password=password,
        )


class Job:
    """Representation of pipeline processing job.

    A job is a named & ordered sequence of processes to execute in a pipeline.
    """

    job_id: int
    """ID for job, as found in Job table of the run results database."""
    name: str
    """Name of the job."""
    procedures: List[FunctionType]
    """Sequence of procedures attached to job."""
    run_id: Union[int, None] = None
    """ID for job run, as found in Job_Run table of the run results database.

    If run has not yet been initiated, value is None.
    """

    def __init__(
        self, name: str, procedures: Optional[Iterable[Callable]] = None
    ) -> None:
        """Initialize instance.

        Args:
            name: Name of the job.
            procedures: Sequence of procedures attached to job
        """
        self.name = name
        self.procedures = list(procedures) if procedures is not None else []
        self._conn = connect(RUN_RESULTS_DB_PATH)
        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute("SELECT id FROM Job WHERE name = ?;", [self.name])
            self.job_id = cursor.fetchone()[0]
        LOG.info("Initialized job instance for `%s`.", self.name)

    @property
    def run_status(self) -> Union[int, None]:
        """Run status code for job-run, as found in Job_Run table.

        If run has not yet been initiated, value is None.
        """
        if self.run_id is None:
            return None

        with self._conn:
            cursor = self._conn.cursor()
            cursor.execute("SELECT status FROM Job_Run WHERE id = ?;", [self.run_id])
            return cursor.fetchone()[0]

    @run_status.setter
    def run_status(self, value: int) -> None:
        if value not in RUN_STATUS_DESCRIPTION:
            raise ValueError(f"{value} not a valid status code")

        if self.run_id is None:
            start_time = _datetime.now().isoformat(" ")
            with self._conn:
                self._conn.execute(
                    "INSERT INTO Job_Run(status, job_id, start_time) VALUES (?, ?, ?);",
                    [value, self.job_id, start_time],
                )
            with self._conn:
                cursor = self._conn.cursor()
                cursor.execute(
                    "SELECT id FROM Job_Run WHERE job_id = ? AND start_time = ?;",
                    [self.job_id, start_time],
                )
                self.run_id = cursor.fetchone()[0]
        else:
            end_time = None if value == -1 else _datetime.now().isoformat(" ")
            with self._conn:
                self._conn.execute(
                    "UPDATE Job_Run SET status = ?, end_time = ? WHERE id = ?;",
                    [value, end_time, self.run_id],
                )


class Pipeline:
    """Representation of a processing pipeline."""

    members: tuple
    """Sequence of executable members attached to pipeline."""

    @staticmethod
    def init_logger(
        member_name: str, file_mode: str = "a", file_level: int = INFO
    ) -> Logger:
        """Initialize & return logger.

        Args:
            member_name: Name of pipeline member.
            file_mode: File mode to write logfile in.
            file_level: Log level above which to log to file.
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
        LOGS_PATH.mkdir(parents=True, exist_ok=True)
        log_path = LOGS_PATH / f"{member_name}.log"
        file_handler = FileHandler(filename=log_path, mode=file_mode)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def __init__(self, *members: Union[FunctionType, Job]) -> None:
        """Initialize instance.

        Args:
            *members: Sequence of executable members attached to pipeline.
        """
        self.members = members

    def execute(self) -> None:
        """Execute pipeline members.

        Raises:
            TypeError: If a member is an invalid member type.
        """
        for member in self.members:
            start_time = _datetime.now()
            if isinstance(member, Job):
                member_name = member.name
                member_type = "job"
                procedures = member.procedures
                member.run_status = -1
            # Callables are assumed to be procedures.
            elif isinstance(member, FunctionType):
                member_name = getattr(member, "__name__", "Unnamed Procedure")
                member_type = "procedure"
                procedures = [member]
            else:
                raise TypeError("Invalid pipeline member type")

            log = self.init_logger(member_name, file_mode="w", file_level=DEBUG)
            log.info("Starting %s: %s.", member_type, member_name)
            for procedure in procedures:
                try:
                    procedure()
                except Exception:
                    log.exception("Unhandled exception")
                    raise

            if member_type == "job":
                member.run_status = 1
            time_elapsed(start_time, logger=log)
            log.info("%s %s.", member_name, RUN_STATUS_DESCRIPTION[1])


def run_as_main(available_members: Mapping[str, Any]) -> None:
    """Script execution code for running as __main__.

    Args:
        available_members: Mapping of names to object for available members to run.
    """
    args = ArgumentParser()
    args.add_argument("members", nargs="*", help="Pipeline member(s) to run")
    available_names = set(available_members)
    member_names = args.parse_args().members
    if member_names and available_names.issuperset(member_names):
        members = [available_members[arg] for arg in member_names]
        pipeline = Pipeline(*members)
        pipeline.execute()
    else:
        console = StreamHandler()
        LOG.addHandler(console)
        if not member_names:
            LOG.error("No pipeline member arguments.")
        for name in member_names:
            if name not in available_names:
                LOG.error("`%s` not available in exec.", name)
        LOG.error(
            "Available objects in exec: %s",
            ", ".join(f"`{name}`" for name in sorted(available_names)),
        )
