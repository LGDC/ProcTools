"""File system objects."""
from collections import Counter

from contextlib import ContextDecorator
import datetime
import filecmp
import logging
from pathlib import Path
import shutil
import stat
import subprocess
from zipfile import ZIP_DEFLATED, BadZipfile, ZipFile

# Py3.7: pairwise added to standard library itertools in 3.10.
from more_itertools import pairwise

from .misc import (  # pylint: disable=relative-beyond-top-level
    elapsed,
    log_entity_states,
)


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

SEVEN_ZIP_PATH = Path(__file__).parent.parent / "resources\\apps\\7_Zip\\x64\\7za.exe"
"""str: Path to 7-Zip command-line app."""


class NetUse(ContextDecorator):
    """Simple manager for network connections.

    Attributes:
        path (pathlib.Path): Path to share.
    """

    def __init__(self, unc_path, username=None, password=None):
        """Initialize instance.

        Args:
            unc_path (pathlib.Path, str): Path to the UNC share.
            username (str): Credential user name.
            password (str): Credential password.
        """
        self.path = Path(unc_path)
        self.__credential = {"username": username, "password": password}

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()

    def __str__(self):
        return str(self.path)

    def connect(self):
        """Connects the UNC directory."""
        # UNC WindowsPath objects keep trailing slash - not compatible with net use.
        string_path = str(self.path).rstrip("\\")
        call_string = f"""net use "{string_path}\""""
        if self.__credential["password"]:
            call_string += f""" {self.__credential["password"]}"""
        if self.__credential["username"]:
            call_string += f""" /user:"{self.__credential["username"]}\""""
        subprocess.check_call(call_string)

    def disconnect(self):
        """Disconnects the UNC directory."""
        call_string = f"""net use "{self.path}" /delete /yes"""
        try:
            subprocess.check_call(call_string)
        except subprocess.CalledProcessError as disconnect_error:
            if disconnect_error.returncode == 2:
                LOG.debug("Network resource %s already disconnected.", self.path)


def archive_folder(folder_path, archive_path, include_base_folder=False, **kwargs):
    """Create zip archive of files in the given folder.

    Args:
        folder_path (pathlib.Path, str): Path of folder to archive.
        archive_path (pathlib.Path, str): Path of archive to create.
        include_base_folder (bool): Have archive include base folder in the file archive
            paths if True.

    Keyword Args:
        archive_exclude_patterns (iter): Collection of file/folder name patterns to
            exclude from archive.
        encrypt_password (str): Password for an encrypted wrapper archive to place the
            folder archive inside. Default is None (no encryption/wrapper).

    Returns:
        pathlib.Path: Path of archive created.
    """
    folder_path = Path(folder_path)
    archive_path = Path(archive_path)
    kwargs.setdefault("archive_exclude_patterns", [])
    kwargs.setdefault("encrypt_password")
    with ZipFile(archive_path, mode="w", compression=ZIP_DEFLATED) as archive:
        for filepath in folder_filepaths(folder_path):
            if any(
                pattern.lower() in str(filepath.relative_to(folder_path)).lower()
                for pattern in kwargs["archive_exclude_patterns"]
            ):
                continue

            archive_filepath = filepath.relative_to(
                folder_path.parent if include_base_folder else folder_path
            )
            archive.write(filename=filepath, arcname=archive_filepath)
    if kwargs["encrypt_password"]:
        encrypted_path = archive_path.parent / ("ENCRYPTED_" + archive_path.name)
        # Usage: 7za.exe <command> <archive_name> [<file_names>...] [<switches>...]
        subprocess.check_call(
            f"""{SEVEN_ZIP_PATH} a "{encrypted_path}" "{archive_path}\""""
            f""" -p"{kwargs["encrypt_password"]}\""""
        )
        archive_path.unlink()
        encrypted_path.rename(archive_path)
    return archive_path


def create_folder(folder_path, create_parents=False, exist_ok=False):
    """Create folder at given path.

    Args:
        folder_path (pathlib.Path, str): Path of folder to create.
        create_parents (bool): Function will create missing parent folders if True,
            will not (and raise FileNotFoundError if missing) if False.
        exist_ok (bool): Already-existing folder treated as successfully created if
            True, raises FileExistsError if False.

    Returns:
        pathlib.Path: Path of the created folder.
    """
    folder_path = Path(folder_path)
    folder_path.mkdir(parents=create_parents, exist_ok=exist_ok)
    return folder_path


def date_file_modified(filepath):
    """Return modified date-time from given filepath.

    Will return None if file does not exist.

    Args:
        filepath (pathlib.Path, str): Path to file.

    Returns:
        datetime.datetime, None
    """
    filepath = Path(filepath)
    if filepath.is_file():
        result = datetime.datetime.fromtimestamp(filepath.stat().st_mtime)
    else:
        result = None
    return result


def extract_archive(archive_path, extract_path, password=None):
    """Extract files from archive into the extract path.

    Args:
        archive_path (pathlib.Path, str): Path of archive file.
        extract_path (pathlib.Path, str): Path of folder to extract into.
        password (str): Password for any encrypted contents.

    Returns:
        bool: True if archived extracted, False otherwise.
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


def flattened_path(path, separator_replacement="_"):
    """Returns "flattened" version of given path, with no separators.

    Args:
        path (pathlib.Path, str): Path to flatten.
        separator_replacement (str): String to replace separators with.

    Returns
        str
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


def folder_filepaths(folder_path, top_level_only=False, **kwargs):
    """Generate paths for files in folder.

    Args:
        folder_path (pathlib.Path, str): Path for folder to list file paths within.
        top_level_only (bool): Only yield paths for files at top-level if True; include
            subfolders as well if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        file_extensions (iter): Collection of file extensions to filter files. Include
            the period in the extension: `.ext`. Use empty string "" for files without
            an extension.

    Yields:
        pathlib.Path
    """
    folder_path = Path(folder_path)
    if kwargs.get("file_extensions"):
        kwargs["file_extensions"] = {ext.lower() for ext in kwargs["file_extensions"]}
    for child_path in folder_path.iterdir():
        if child_path.is_file():
            if (
                not kwargs.get("file_extensions")
                or child_path.suffix.lower() in kwargs["file_extensions"]
            ):
                yield child_path
        elif child_path.is_dir() and not top_level_only:
            yield from folder_filepaths(child_path, top_level_only, **kwargs)


def same_file(*filepaths, not_exists_ok=True):
    """Determine if given files are the same.

    Args:
        *filepaths (iter of pathlib.Path or str): Collection of filepaths of files to
            compare.
        not_exists_ok (bool): True if a path for a nonexistent file should be treated as
            a file and as "different" than any actual files.

    Returns:
        bool
    """
    filepaths = {Path(filepath) for filepath in filepaths}
    if any(not filepath.is_file() for filepath in filepaths):
        if not_exists_ok:
            return False

        raise FileNotFoundError("One or more nonexistant files (not_exists_ok=False)")

    return all(
        filecmp.cmp(filepath, cmp_filepath)
        for filepath, cmp_filepath in pairwise(filepaths)
    )


def update_file(filepath, source_filepath):
    """Update file from source.

    Args:
        filepath (pathlib.Path, str): Path to file to be updated.
        source_filepath (pathlib.Path, str): Path to source file.

    Returns:
        str: Result key--"created", "failed to create", "updated", "failed to update",
        or "no update necessary".
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
                filepath.chmod(mode=stat.S_IWRITE)
            try:
                shutil.copy2(source_filepath, filepath)
            except IOError:
                result = "failed to update"
            else:
                result = "updated"
    else:
        # Create folder structure (if necessary).
        filepath.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(source_filepath, filepath)
        except IOError:
            result = "failed to create"
        else:
            result = "created"
    if result in ["created", "updated"]:
        filepath.chmod(mode=stat.S_IWRITE)
        level = logging.INFO
    elif "failed to" in result:
        level = logging.WARNING
    else:
        level = logging.DEBUG
    LOG.log(level, "`%s` %s from `%s`.", filepath, result, source_filepath)
    return result


def update_replica_folder(folder_path, source_path, top_level_only=False, **kwargs):
    """Update replica folder from source.

    Args:
        folder_path (pathlib.Path, str): Path to replica folder.
        source_path (pathlib.Pathm, str): Path to source folder.
        top_level_only (bool): Only update files at top-level of folder if True; include
            subfolders as well if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        file_extensions (iter): Collection of file extensions to filter files. Include
            the period in the extension: `.ext`. Use empty string "" for files without
            an extension.
        flatten_tree (bool): If True, replica repository will be updated with files
            "flattened" into the root folder, regardless of where in the source
            hierarchy they reside. Default is False.
        logger (logging.Logger): Logger to emit loglines to. If not defined will default
            to submodule logger.
        log_evaluated_division (int): Division at which to emit a logline about number
            of files evaluated so far. If not defined or None, will default to not
            logging evaluated divisions.

    Returns:
        collections.Counter: Counts for each update result type: "updated", "failed to
            update", or "no update necessary"
    """
    start_time = datetime.datetime.now()
    folder_path = Path(folder_path)
    source_path = Path(source_path)
    kwargs.setdefault("flatten_tree", False)
    kwargs.setdefault("log_evaluated_division", -1)
    log = kwargs.get("logger", LOG)
    log.info("Start: Update replica folder `%s` from `%s`.", folder_path, source_path)
    for repository_path in (folder_path, source_path):
        if not repository_path.is_dir():
            raise FileNotFoundError(f"`{repository_path}` not accessible folder")

    states = Counter()
    source_filepaths = folder_filepaths(
        source_path, top_level_only, file_extensions=kwargs.get("file_extensions")
    )
    for i, source_filepath in enumerate(source_filepaths, start=1):
        if kwargs["flatten_tree"]:
            filepath = folder_path / source_filepath.name
        else:
            filepath = folder_path / source_filepath.relative_to(source_path)
            # Add folder (if necessary).
            filepath.parent.mkdir(parents=True, exist_ok=True)
        states[update_file(filepath, source_filepath)] += 1
        if (
            kwargs["log_evaluated_division"] > 0
            and i % kwargs["log_evaluated_division"] == 0
        ):
            log.info(f"Evaluated {i:,} files.")
    log_entity_states("files", states, logger=log, log_level=logging.INFO)
    elapsed(start_time, logger=log)
    log.info("End: Update.")
    return states
