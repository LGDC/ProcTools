"""Metadata objects."""
from dataclasses import asdict, dataclass, field
from datetime import date, datetime as _datetime
from itertools import chain
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Iterable, List, Optional, Union
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

import arcproc

from proctools.misc import sql_server_odbc_string


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""


@dataclass
class Database:
    """Representation of database information."""

    name: str
    """Name of the database."""
    hostname: str
    """Name of database instance host."""
    port: Optional[int] = None
    """Port to connect to instance on."""
    data_schema_names: Iterable[str] = field(default_factory=set)
    """Collection of data schema names.

    Often used to identify which owned schemas need compressing.
    """

    def __post_init__(self) -> None:
        """Post-initialization."""
        self.data_schema_names = set(self.data_schema_names)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, host={self.host!r})"

    @property
    def host(self) -> str:
        """Name & port configuration of database instance host."""
        return f"{self.hostname},{self.port}"

    def create_session(
        self,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        application_name: Optional[str] = None,
        read_only: bool = False,
    ) -> Session:
        """Return SQLAlchemy session instance to database.

        Args:
            username: Name of user for authentication with instance.
            password: Password for authentication with instance.
            application_name: Name of application to represent connection as being from.
            read_only: Application intent is for read-only workload if True.
        """
        odbc_string = self.get_odbc_string(
            username=username,
            password=password,
            application_name=application_name,
            read_only=read_only,
        )
        url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_string)}"
        engine = create_engine(url)
        return sessionmaker(bind=engine)()

    def get_odbc_string(
        self,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        application_name: Optional[str] = None,
        read_only: bool = False,
    ) -> str:
        """Return String necessary for ODBC connection.

        Args:
            username: Name of user for authentication with instance.
            password: Password for authentication with instance.
            application_name: Name of application to represent connection as being from.
            read_only: Application intent is for read-only workload if True.
        """
        return sql_server_odbc_string(
            host=self.host,
            database_name=self.name,
            username=username,
            password=password,
            application_name=application_name,
            read_only=read_only,
        )


@dataclass
class Field:
    """Representation of field information."""

    name: str
    """Name of the field."""
    type: str = "String"
    """Field type (case insensitve). See `valid_types` property for possible values. """
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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, type={self.type!r})"


@dataclass
class Dataset:
    """Representation of dataset information."""

    fields: List[Field] = field(default_factory=list)
    """Dataset field information objects."""
    geometry_type: Optional[str] = None
    """Type of geometry. NoneType indicates nonspatial."""
    path: Optional[Path] = None
    """Path to dataset."""
    source_path: Optional[Path] = None
    "Path to source dataset."
    source_paths: Optional[List[Path]] = field(default_factory=list)
    "Paths to source datasets."

    def __fspath__(self) -> str:
        return str(self.path)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(path={self.path!r})"

    @property
    def field_names(self) -> List[str]:
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
    def id_field_names(self) -> List[str]:
        """Dataset identifier field names."""
        return [field.name for field in self.id_fields]

    @property
    def id_fields(self) -> List[Field]:
        """Dataset identifier field information objects."""
        return [field for field in self.fields if field.is_id]

    @property
    def out_field_names(self) -> List[str]:
        """Output dataset field names."""
        return [field.name for field in self.out_fields]

    @property
    def out_fields(self) -> List[Field]:
        """Output dataset field information objects."""
        return [field for field in self.fields if not field.source_only]

    @property
    def source_field_names(self) -> List[str]:
        """Source dataset field names."""
        return [field.name for field in self.source_fields]

    @property
    def source_fields(self) -> List[Field]:
        """Source dataset field information objects."""
        return [
            field
            for field in self.fields
            if field.source_only or not field.not_in_source
        ]

    def create(
        self,
        *,
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

        Returns:
            Path to dataset.
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
    dataset_path: Union[Path, str], date_changed_field_names: Iterable[str]
) -> Union[date, _datetime, None]:
    """Return date of the last change on dataset with tracking fields.

    Args:
        dataset_path: Path to dataset.
        change_date_field_names: Names of fields that record change-dates.
    """
    date_tuples = arcproc.features.as_tuples(
        dataset_path, field_names=date_changed_field_names
    )
    dates = {_date for _date in chain.from_iterable(date_tuples) if _date}
    return max(dates) if dates else None