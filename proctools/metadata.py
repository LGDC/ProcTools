"""Metadata objects."""
from dataclasses import asdict, dataclass, field
from itertools import chain
import logging
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import arcproc

from proctools.misc import sql_server_odbc_string


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


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
        if "application" in kwargs:
            kwargs["application_name"] = kwargs.pop("application")
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
            self.host, self.name, username=username, password=password, **kwargs
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
