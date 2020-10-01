"""Metadata objects."""
import datetime
from functools import partial
from itertools import chain
import logging
from operator import itemgetter
import os
import sqlite3
import sys
from types import FunctionType

try:
    from urllib.parse import quote_plus
except ImportError:
    # Py2.
    from urllib import quote_plus

from jinja2 import Environment, PackageLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# import arcproc  # Imported locally to avoid slow imports.
from .communicate import extract_email_addresses, send_email_smtp
from .filesystem import create_folder
from .misc import sql_server_odbc_string
from .value import datetime_from_string

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
                    run_meta[key] = datetime_from_string(run_meta[key])
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

    @property
    def start_times(self):
        """set of tuples: Collection of tuples containing start & ."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "select start_time from Last_Job_Run where batch_id = ?;"
            cursor.execute(sql, [self.id])
            times = {datetime_from_string(row[0]) for row in cursor}
            if None in times:
                times.remove(None)
        return times

    @property
    def status(self):
        """int: status ID for current batch run."""
        with self._conn:
            cursor = self._conn.cursor()
            sql = "select status from Last_Job_Run where batch_id = ?;"
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


class Database(object):
    """Representation of database information.

    Attributes:
        data_schema_names (set): Collection of data schema names.
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
        return "{}(name={!r}, host={!r})".format(
            self.__class__.__name__, self.name, self.host
        )

    @property
    def hostname(self):
        """str: Name of the instance host."""
        return self.host.split(",")[0]

    def create_session(self, username=None, password=None, **kwargs):
        """Return SQLAlchemy session instance to database.

        Returns:
            sqlalchemy.orm.session.Session: Session object connected to the database.

        Keyword Args:
            See keyword args listed for `sql_server_odbc_string` function.
        """
        odbc_string = self.get_odbc_string(username, password, **kwargs)
        url = self._sqlalchemy.setdefault(
            "url", "mssql+pyodbc:///?odbc_connect={}".format(quote_plus(odbc_string)),
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

    def attributes_as_dicts(self, path_tag=None, field_names=None, **kwargs):
        """Generate mappings of feature attribute name to value.

        Notes:
            Use ArcPy cursor token names for object IDs and geometry objects/properties.

        Args:
            path_tag (str): Tag for the path to generate attributes from.
            field_names (iter): Collection of field names. Names will be the keys in the
                dictionary mapping to their values. If value is None, all attributes
                fields will be used.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            Refer to Keyword Args for `arcproc.attributes.as_dicts`.

        Yields:
            dict.
        """
        import arcproc

        features = arcproc.attributes.as_dicts(
            dataset_path=self.path(path_tag), field_names=field_names, **kwargs
        )
        for feature in features:
            yield feature

    def attributes_as_iters(self, path_tag=None, field_names=None, **kwargs):
        """Generate iterables of feature attribute values.

        Notes:
            Use ArcPy cursor token names for object IDs and geometry objects/properties.

        Args:
            path_tag (str): Tag for the path to generate attributes from.
            field_names (iter): Collection of field names. The order of the names in
                the collection will determine where its value will fall in the generated
                item. If value is None, all attributes fields will be used, in
                `self.field_names` order.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            Refer to Keyword Args for `arcproc.attributes.as_iters`.

        Yields:
            iter.
        """
        import arcproc

        if not field_names:
            field_names = self.field_names
        features = arcproc.attributes.as_dicts(
            dataset_path=self.path(path_tag), field_names=field_names, **kwargs
        )
        for feature in features:
            yield feature

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
        import arcproc

        # Check for path in path-tags; otherwise assume path is literal.
        dataset_path = self._path.get(path, path)
        field_metadata_list = (
            [field for field in self.fields if field_tag in field["tags"]]
            if field_tag
            else self.fields
        )
        arcproc.dataset.create(
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
        if tag not in self._path:
            raise AttributeError("{!r} path does not exist.".format(tag))

        return self._path[tag]


class Job(object):
    """Representation of pipeline processing job.

    A job is an named &ordered sequence of processes to execute in a pipeline.

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
        else:
            end_time = None if value == -1 else datetime.datetime.now().isoformat(" ")
            with self._conn:
                sql = """
                    update Job_Run set status = ?, end_time = ? where id = ?;
                """
                self._conn.execute(sql, [value, end_time, self.run_id])


class Pipeline(object):
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
        logger.setLevel(logging.DEBUG)
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
        create_folder(LOGS_PATH, exist_ok=True, create_parents=True)
        file_handler = logging.FileHandler(
            filename=os.path.join(LOGS_PATH, member_name + ".log"), mode=file_mode
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
                raise ValueError("Invalid pipeline member type.")

            log = self.init_logger(meta["name"], file_mode="w", file_level=10)
            log.info("Starting %s: %s.", meta["type"], meta["name"])
            for procedure in meta["procedures"]:
                try:
                    procedure()
                except Exception:
                    log.exception("Unhandled exception.")
                    raise

            meta["status"] = 1
            if meta["type"] == "job":
                member.run_status = meta["status"]
            log.info("%s %s.", meta["name"], RUN_STATUS_DESCRIPTION[meta["status"]])


def dataset_last_change_date(
    dataset_path, init_date_field_name="init_date", mod_date_field_name="mod_date"
):
    """Return date of the last change on dataset with tracking fields."""
    import arcproc

    field_names = [init_date_field_name, mod_date_field_name]
    date_iters = arcproc.attributes.as_iters(dataset_path, field_names)
    dates = set(chain.from_iterable(date_iters))
    # datetimes cannot compare to NoneTypes.
    if None in dates:
        dates.remove(None)
    return max(dates) if dates else None
