"""Metadata objects."""
import datetime
from itertools import chain
import logging
from operator import itemgetter
import os
import sqlite3
import sys

try:
    from urllib.parse import quote_plus
except ImportError:
    # Py2.
    from urllib import quote_plus

from jinja2 import Environment, PackageLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# import arcetl  # Imported locally to avoid slow imports.
from .communicate import extract_email_addresses, send_email_smtp
from .misc import sql_server_odbc_string

# Py2.
if sys.version_info.major >= 3:
    basestring = str  # pylint: disable=invalid-name


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

PROC_PATH = os.getenv("PROC_PATH", os.path.join(os.getenv("LOCALAPPDATA"), "proc"))
"""str: Path to folder for processing environment."""
LOGS_PATH = os.path.join(PROC_PATH, "logs")
"""str: Path to folder for logging content."""
RUN_RESULTS_DB_PATH = os.path.join(LOGS_PATH, "Run_Results.sqlite3")
"""str: Path for execution run-result database."""
RUN_STATUS_DESCRIPTION = {1: "complete", 0: "failed", -1: "incomplete"}
"""dict: Mapping of status code to description."""


class Batch(object):
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
            sql = "select id from Batch where name = ?;"
            cursor.execute(sql, [self.name])
            return cursor.fetchone()[0]

    @property
    def job_names(self):
        """list of str: Names of the jobs assigned to batch."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "select name from Job where batch_id = ?;"
            cursor.execute(sql, [self.id])
            return [name for name, in cursor.fetchall()]

    @property
    def last_job_run_metas(self):
        """list of dict: Metadata dictionaries for last job-runs."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "select * from Last_Job_Run where batch_id = ?;"
            cursor.execute(sql, [self.id])
            metas = [
                {column[0]: value for column, value in zip(cursor.description, row)}
                for row in cursor
            ]
            # Coerce timestamps to datetime--no sqlite3 date/time types, using text.
            for run_meta in metas:
                for key in ["start_time", "end_time"]:
                    run_meta[key] = datetime.datetime.strptime(
                        run_meta[key], "%Y-%m-%d %H:%M:%S.%f"
                    )
            return metas

    @property
    def notification_addresses(self):
        """dict: Mapping of type to list of email addresses for notification."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = """
                select
                    notification_to_addresses as 'to_addresses',
                    notification_copy_addresses as 'copy_addresses',
                    notification_blind_copy_addresses as 'blind_copy_addresses',
                    notification_reply_to_addresses as 'reply_to_addresses'
                from Batch where name = ?
                limit 1;
            """
            row = cursor.execute(sql, [self.name]).fetchone()
            if not row:
                raise ValueError("Batch name not valid member of Batch table.")

            addresses = {
                column[0]: list(extract_email_addresses(value))
                for column, value in zip(cursor.description, row)
            }
            return addresses

    def send_notification(self, host, from_address, **kwargs):
        """Send email notification for batch.

        Args:
            host (str, None): Host name of SMTP server.
            from_address (str): Email address for sender.
            body (str): Message body text.
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
            batch_status = (
                1 if all(run["status"] == 1 for run in last_run_metas) else -1
            )
            kwargs.update(self.notification_addresses)
            send_email_smtp(
                host,
                from_address,
                subject="Processing Batch: {} ({})".format(
                    self.name, RUN_STATUS_DESCRIPTION[batch_status]
                ),
                body=template.render(last_run_metas=last_run_metas),
                body_type="html",
                **kwargs
            )


class Database(object):
    """Representation of database information.

    Attributes:
        name (str): Name of the database.
        host (str): Name of the SQL Server instance host.
        path (str): SDE-style path to database.
        data_schema_names (set): Collection of data schema names.
    """

    def __init__(self, name, host, **kwargs):
        """Initialize instance.

        Args:
            name (str): Name of the database.
            host (str): Name of the SQL Server instance host.
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
        return "{}(name={!r}, host={!r})".format(
            self.__class__.__name__, self.name, self.host
        )

    def create_session(self, username=None, password=None, **kwargs):
        """Return SQLAlchemy session instance to database.

        Returns:
            sqlalchemy.orm.session.Session: Session object connected to the database.

        Keyword Args:
            See keyword args listed for `sql_server_odbc_string` function.
        """
        url = self._sqlalchemy.setdefault(
            "url",
            "mssql+pyodbc:///?odbc_connect={}".format(
                quote_plus(self.get_odbc_string(username, password, **kwargs))
            ),
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
            application (str): Name of application to represent connection as being from
                (optional).

        Keyword Args:
            See keyword args listed for `sql_server_odbc_string` function.

        Returns:
            str
        """
        return sql_server_odbc_string(
            self.host, self.name, username, password, **kwargs
        )


class Dataset(object):
    """Representation of dataset information."""

    valid_geometry_types = ["point", "multipoint", "polygon", "polyline"]

    @staticmethod
    def init_tag_property(value):
        """Initialize a tag-style property mapping.

        Args:
            value (dict, str): Value to initialize into tag-property.

        Returns:
            dict
        """
        if isinstance(value, dict):
            return value

        if isinstance(value, basestring):
            return {None: value}

        raise TypeError("Invalid type for tag property.")

    def __init__(self, fields, geometry_type=None, path=None):
        """Initialize instance.

        Args:
            fields (iter): Collection of field information dictionaries.
            geometry_type (str, None): Type of geometry. NoneType indicates nonspatial.
            path (dict, str): Path or tagged mapping of paths.
        """
        self._geometry_type = None
        self.geometry_type = geometry_type
        self.fields = list(fields)
        self._path = self.init_tag_property(path)

    @property
    def field_names(self):
        """list of str: Dataset field names."""
        return [field["name"] for field in self.fields]

    @property
    def geometry_type(self):
        """str, None: Dataset geometry type."""
        return self._geometry_type

    @geometry_type.setter
    def geometry_type(self, value):
        if value is None:
            self._geometry_type = value
        elif value.lower() in Dataset.valid_geometry_types:
            self._geometry_type = value.lower()

    @property
    def id_field_names(self):
        """list of str: Dataset identifier field names."""
        return [field["name"] for field in self.fields if field.get("is_id")]

    @property
    def id_fields(self):
        """list of dict: Dataset identifier field info dictionaries."""
        return [field for field in self.fields if field["is_id"]]

    def add_path(self, tag, path):
        """Add a path for the given tag.

        Args:
            tag (str): Path tag.
            path (str): Path to add.
        """
        self._path[tag] = path

    def create(self, path, field_tag=None, spatial_reference_item=None):
        """Create dataset from instance properties.

        Args:
            path (str): Path for dataset to create.
            field_tag (str, None): Tag for fields to add to created dataset. If None,
                all fields listed in self.fields will be added.
            spatial_reference_item (object): Object with which to define the spatial
                reference from. If None, dataset created will be nonspatial.

        Returns:
            str: Path of dataset created.
        """
        import arcetl

        # Check for path in path-tags; otherwise assume path is literal.
        dataset_path = self._path.get(path, path)
        field_metadata_list = (
            [field for field in self.fields if field_tag in field["tags"]]
            if field_tag
            else self.fields
        )
        arcetl.dataset.create(
            dataset_path,
            field_metadata_list,
            geometry_type=self.geometry_type,
            spatial_reference_item=spatial_reference_item,
        )
        return dataset_path

    def path(self, tag=None):
        """Return path string associated with the given tag.

        Args:
            tag (str): Path tag.

        Returns:
            str: Path of dataset with given tag.
        """
        return self._path[tag]


class Job(object):
    """Representation of pipeline processing job.

    A job is an named &ordered sequence of processes to execute in a pipeline.

    Attributes:
        name (str): Name of the job.
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
        self._procedures = []
        self.procedures = procedures
        self.run_id = None
        self._conn = sqlite3.connect(RUN_RESULTS_DB_PATH)
        LOG.info("Initialized job instance for `%s`.", self.name)

    @property
    def id(self):  # pylint: disable=invalid-name
        """int: ID for job, as found in Job table."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "select id from Job where name = ?;"
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
            sql = "select status from Job_Run where id = ?;"
            cursor.execute(sql, [self.run_id])
            return cursor.fetchone()[0]

    @run_status.setter
    def run_status(self, value):
        if value not in RUN_STATUS_DESCRIPTION:
            raise ValueError("{} not a valid status code.".format(value))

        if self.run_id is None:
            start_time = datetime.datetime.now().isoformat(" ")
            with self._conn:
                sql = """
                    insert into Job_Run(status, job_id, start_time) values (?, ?, ?);
                """
                self._conn.execute(sql, [value, self.id, start_time])
            with self._conn:
                cursor = self._conn.cursor()
                sql = "select id from Job_Run where job_id = ? and start_time = ?;"
                cursor.execute(sql, [self.id, start_time])
                self.run_id = cursor.fetchone()[0]
        elif value in [-1]:
            with self._conn:
                sql = "update Job_Run set status = ? where run_id = ?;"
                self._conn.execute(sql, [value, self.run_id])
        else:
            end_time = datetime.datetime.now().isoformat(" ")
            with self._conn:
                sql = """
                    update Job_Run set status = ?, end_time = ? where id = ?;
                """
                self._conn.execute(sql, [value, end_time, self.run_id])

    @property
    def procedures(self):
        """list: Ordered collection of procedures attached to job."""
        return self._procedures

    @procedures.setter
    def procedures(self, value):
        if value is None:
            self._procedures = []
        else:
            self._procedures = list(value)
def dataset_last_change_date(
    dataset_path, init_date_field_name="init_date", mod_date_field_name="mod_date"
):
    """Return date of the last change on dataset with tracking fields."""
    import arcetl

    field_names = [init_date_field_name, mod_date_field_name]
    date_iters = arcetl.attributes.as_iters(dataset_path, field_names)
    dates = set(chain.from_iterable(date_iters))
    # datetimes cannot compare to NoneTypes.
    if None in dates:
        dates.remove(None)
    return max(dates) if dates else None
