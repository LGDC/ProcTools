"""File system objects."""
from collections import Counter
from contextlib import ContextDecorator
from datetime import datetime as _datetime
from filecmp import cmp
from logging import DEBUG, INFO, WARNING, Logger, getLogger
from pathlib import Path
from shutil import copy2
from stat import S_IWRITE
from subprocess import CalledProcessError, check_call
from types import TracebackType
from typing import Iterable, Iterator, Optional, Type, TypeVar, Union
from zipfile import ZIP_DEFLATED, BadZipfile, ZipFile

from more_itertools import pairwise

from proctools.misc import log_entity_states, time_elapsed


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""

# Py3.7: Can replace usage with `typing.Self` in Py3.11.
TNetUse = TypeVar("TNetUse", bound="NetUse")
"""Type variable to enable method return of self on NetUse."""


class NetUse(ContextDecorator):
    """Simple manager for network resource connections."""

    unc_path: str
    """UNC path to network resource."""

    def __init__(
        self,
        unc_path: Union[Path, str],
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """Initialize instance.

        Args:
            unc_path: Path to the UNC share.
            username: Name of user for authentication with resource.
            password: Password for authentication with resource.
        """
        self.unc_path = Path(unc_path)
        self.__username = username
        self.__password = password

    def __enter__(self) -> TNetUse:
        self.connect()
        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        self.disconnect()

    def __str__(self) -> str:
        # UNC WindowsPath objects keep trailing slash - not compatible with net use.
        return str(self.unc_path).rstrip("\\")

    def connect(self) -> None:
        """Connect to resource."""
        call_string = f"""net use "{self}\""""
        if self.__password:
            call_string += f" {self.__password}"
        if self.__username:
            call_string += f""" /user:"{self.__username}\""""
        check_call(call_string)

    def disconnect(self) -> None:
        """Disconnect resource."""
        call_string = f"""net use "{self}" /delete"""
        try:
            check_call(call_string)
        except CalledProcessError as disconnect_error:
            if disconnect_error.returncode == 2:
                LOG.debug("Network resource `%s` already disconnected.", self.unc_path)


def archive_folder(
    folder_path: Union[Path, str],
    *,
    archive_path: Union[Path, str],
    exclude_patterns: Optional[Iterable[str]] = None,
    include_base_folder: bool = False,
) -> Path:
    """Create zip archive of files in the given folder.

    Args:
        folder_path: Path to folder.
        archive_path: Path to archive.
        exclude_patterns (iter): Collection of file/folder name patterns to
            exclude from archive.
        include_base_folder: If True file archive paths will include the base folder.

    Returns:
        Path to archive.
    """
    folder_path = Path(folder_path)
    archive_path = Path(archive_path)
    exclude_patterns = list(exclude_patterns) if exclude_patterns else []
    with ZipFile(archive_path, mode="w", compression=ZIP_DEFLATED) as archive:
        for filepath in folder_filepaths(folder_path):
            if any(
                pattern.casefold() in str(filepath.relative_to(folder_path)).casefold()
                for pattern in exclude_patterns
            ):
                continue

            archive_filepath = filepath.relative_to(
                folder_path.parent if include_base_folder else folder_path
            )
            archive.write(filename=filepath, arcname=archive_filepath)
    return archive_path


def date_file_modified(filepath: Union[Path, str]) -> _datetime:
    """Return modified date-time from given filepath.

    Args:
        filepath: Path to file.
    """
    return _datetime.fromtimestamp(Path(filepath).stat().st_mtime)


def extract_archive(
    archive_path: Union[Path, str],
    *,
    extract_path: Union[Path, str],
    password: Optional[str] = None,
) -> bool:
    """Extract files from archive into the extract folder.

    Args:
        archive_path: Path to archive file.
        extract_path: Path to extract folder.
        password: Password for any encrypted contents.

    Returns:
        True if archived extracted, False otherwise.
    """
    archive_path = Path(archive_path)
    extract_path = Path(extract_path)
    try:
        with ZipFile(archive_path, "r") as archive:
            archive.extractall(extract_path, pwd=password)
    except BadZipfile:
        LOG.warning("`%s` not a valid archive.", archive_path)
        extracted = False
    else:
        extracted = True
    return extracted


def flattened_path(path: Union[Path, str], separator_replacement: str = "_") -> str:
    """Returns "flattened" string of given path (separators replaced).

    Args:
        path: Path.
        separator_replacement: String to replace separators with.
    """
    path = str(path)
    for character in ["/", "\\", ":"]:
        path = path.replace(character, separator_replacement)
    while separator_replacement * 2 in path:
        path = path.replace(separator_replacement * 2, separator_replacement)
    while path.startswith(separator_replacement) or path.endswith(
        separator_replacement
    ):
        path = path.strip(separator_replacement)
    return path


def folder_filepaths(
    folder_path: Union[Path, str],
    *,
    file_extensions: Optional[Iterable[str]] = None,
    top_level_only: bool = False,
) -> Iterator[Path]:
    """Generate paths to files in given folder.

    Args:
        folder_path: Path to folder.
        file_extensions: Collection of file extensions for files to include in
            generator. Include the period in the extension, e.g. ".ext". Use an empty
            string "" for files without an extension.
        top_level_only: Only yield paths for files at top-level if True. Include
            subfolders as well if False.
    """
    folder_path = Path(folder_path)
    if file_extensions:
        file_extensions = {ext.casefold() for ext in file_extensions}
    for child_path in folder_path.iterdir():
        if child_path.is_file():
            if not file_extensions or child_path.suffix.casefold() in file_extensions:
                yield child_path

        elif child_path.is_dir() and not top_level_only:
            yield from folder_filepaths(
                child_path,
                file_extensions=file_extensions,
                top_level_only=top_level_only,
            )


def same_file(*filepaths: Union[Path, str], not_exists_ok: bool = True) -> bool:
    """Return True if given files are the same, False if not.

    Args:
        *filepaths (iter of pathlib.Path or str): Collection of filepaths of files to
            compare.
        not_exists_ok (bool): True if a path for a nonexistent file should be treated as
            a file and as "different" than any actual files.
    """
    filepaths = {Path(filepath) for filepath in filepaths}
    if any(not filepath.is_file() for filepath in filepaths):
        if not_exists_ok:
            same = False
        else:
            raise FileNotFoundError(
                "One or more nonexistant files (not_exists_ok=False)"
            )

    elif len(filepaths) <= 1:
        same = True
    else:
        same = all(
            cmp(filepath, cmp_filepath)
            for filepath, cmp_filepath in pairwise(filepaths)
        )
    return same


def update_file(
    filepath: Union[Path, str], *, source_filepath: Union[Path, str]
) -> str:
    """Update file from source.

    Args:
        filepath: Path to file.
        source_filepath: Path to source file.

    Returns:
        Result key--"created", "failed to create", "updated", "failed to update", or
        "no update necessary".
    """
    filepath = Path(filepath)
    source_filepath = Path(source_filepath)
    if not source_filepath.is_file():
        raise FileNotFoundError(f"Source file '{source_filepath}` not extant file.")

    if filepath.exists():
        if same_file(filepath, source_filepath):
            result = "no update necessary"
        else:
            # Make destination file overwriteable.
            if filepath.exists():
                filepath.chmod(mode=S_IWRITE)
            try:
                copy2(source_filepath, filepath)
            except IOError:
                result = "failed to update"
            else:
                result = "updated"
    else:
        # Create folder structure (if necessary).
        filepath.parent.mkdir(parents=True, exist_ok=True)
        try:
            copy2(source_filepath, filepath)
        except IOError:
            result = "failed to create"
        else:
            result = "created"
    if result in ["created", "updated"]:
        filepath.chmod(mode=S_IWRITE)
        log_level = INFO
    elif "failed to" in result:
        log_level = WARNING
    else:
        log_level = DEBUG
    LOG.log(log_level, "`%s` %s from `%s`.", filepath, result, source_filepath)
    return result


def update_replica_folder(
    folder_path: Union[Path, str],
    *,
    source_path: Union[Path, str],
    file_extensions: Optional[Iterable[str]] = None,
    flatten_tree: bool = False,
    top_level_only: bool = False,
    logger: Optional[Logger] = None,
    log_evaluated_division: Optional[int] = None,
) -> Counter:
    """Update folder from source.

    Args:
        folder_path: Path to folder.
        source_path: Path to source folder.
        file_extensions: Collection of file extensions for files to include in update.
            Include the period in the extension, e.g. ".ext". Use an empty string "" for
            files without an extension.
        flatten_tree: If True, replica repository will be updated with files "flattened"
            into the root folder, regardless of where in the source hierarchy they are.
        top_level_only: Only update files  at top-level if True. Include subfolders &
            their filed as well if False.
        logger: Logger to emit loglines to. If set to None, will default to submodule
            logger.
        log_evaluated_division: Division at which to emit a logline about the number of
            files evaluated so far. If set to None, will default to not logging
            divisions.

    Returns:
        File counts for each update result type.
    """
    start_time = _datetime.now()
    folder_path = Path(folder_path)
    source_path = Path(source_path)
    if logger is None:
        logger = LOG
    logger.info("Start: Update folder `%s` from `%s`.", folder_path, source_path)
    for dirpath in [folder_path, source_path]:
        if not dirpath.is_dir():
            raise FileNotFoundError(f"`{dirpath}` not accessible folder")

    source_filepaths = folder_filepaths(
        source_path, file_extensions=file_extensions, top_level_only=top_level_only
    )
    states = Counter()
    for i, source_filepath in enumerate(source_filepaths, start=1):
        if flatten_tree:
            filepath = folder_path / source_filepath.name
        else:
            filepath = folder_path / source_filepath.relative_to(source_path)
            # Add folder (if necessary).
            filepath.parent.mkdir(parents=True, exist_ok=True)
        states[update_file(filepath, source_filepath=source_filepath)] += 1
        if log_evaluated_division and i % log_evaluated_division == 0:
            logger.info("Evaluated %s files.", format(i, ",d"))
    log_entity_states("files", states, logger=logger, log_level=INFO)
    time_elapsed(start_time, logger=logger)
    logger.info("End: Update.")
    return states
