"""File system objects."""
from collections import Counter

try:
    from contextlib import ContextDecorator
except ImportError:
    # Py2.
    from contextlib2 import ContextDecorator
import datetime
import filecmp
import logging
import os
import shutil
import stat
import subprocess
import zipfile

from more_itertools import pairwise

from .misc import elapsed, log_entity_states


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

SEVEN_ZIP_PATH = os.path.join(
    os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir)),
    "resources\\apps\7_Zip\\x64\\7za.exe",
)
"""str: Path to 7-Zip command-line app."""


class NetUse(ContextDecorator):
    """Simple manager for network connections.

    Attributes:
        path (str): Path to share.
    """

    def __init__(self, unc_path, username=None, password=None):
        """Initialize instance.

        Args:
            unc_path (str): Path to the UNC share.
            username (str): Credential user name.
            password (str): Credential password.
        """
        self.path = unc_path
        self.__credential = {"username": username, "password": password}

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()

    def __str__(self):
        return self.path

    def connect(self):
        """Connects the UNC directory."""
        LOG.info("Connecting UNC path %s.", self.path)
        call_string = """net use "{}\"""".format(self.path)
        if self.__credential["password"]:
            call_string += " {}".format(self.__credential["password"])
        if self.__credential["username"]:
            call_string += """ /user:"{}\"""".format(self.__credential["username"])
        subprocess.check_call(call_string)

    def disconnect(self):
        """Disconnects the UNC directory."""
        LOG.info("Disconnecting UNC path %s.", self.path)
        call_string = """net use "{}" /delete /yes""".format(self.path)
        try:
            subprocess.check_call(call_string)
        except subprocess.CalledProcessError as disconnect_error:
            if disconnect_error.returncode == 2:
                LOG.debug("Network resource %s already disconnected.", self.path)


def archive_folder(folder_path, archive_path, directory_as_base=False, **kwargs):
    """Create zip archive of files in the given directory.

    Args:
        directory_path (str): Path of directory to archive.
        archive_path (str): Path of archive to create.
        directory_as_base (bool): Place contents in the base directory within the
            archive if True, do not if False.

    Keyword Args:
        archive_exclude_patterns (iter): Collection of file/folder name patterns to
            exclude from archive.
        encrypt_password (str): Password for an encrypted wrapper archive to place the
            directory archive inside. Default is None (no encryption/wrapper).

    Returns:
        str: Path of archive created.
    """
    kwargs.setdefault("archive_exclude_patterns", [])
    kwargs.setdefault("encrypt_password")
    LOG.info("Start: Create archive of directory %s.", folder_path)
    if directory_as_base:
        directory_root_length = len(os.path.dirname(folder_path)) + 1
    else:
        directory_root_length = len(folder_path) + 1
    archive = zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED)
    with archive:
        for subdirectory_path, _, file_names in os.walk(folder_path):
            if any(
                pattern.lower() in os.path.basename(subdirectory_path).lower()
                for pattern in kwargs["archive_exclude_patterns"]
            ):
                continue

            for file_name in file_names:
                if any(
                    pattern.lower() in file_name.lower()
                    for pattern in kwargs["archive_exclude_patterns"]
                ):
                    continue

                file_path = os.path.join(subdirectory_path, file_name)
                file_archive_path = file_path[directory_root_length:]
                archive.write(file_path, file_archive_path)
    if kwargs["encrypt_password"]:
        out_path = "{}_encrypted{}".format(*os.path.splitext(archive_path))
        # Usage: 7za.exe <command> <archive_name> [<file_names>...] [<switches>...]
        call_string = """{exe} a "{wrapper}" "{archive}" -p"{password}" """.format(
            exe=SEVEN_ZIP_PATH,
            wrapper=out_path,
            archive=archive_path,
            password=kwargs["encrypt_password"],
        )
        subprocess.check_call(call_string)
        os.remove(archive_path)
    else:
        out_path = archive_path
    LOG.info("End: Create.")
    return out_path


def create_folder(folder_path, exist_ok=False, create_parents=False):
    """Create directory at given path.

    Args:
        directory_path (str): Path of directory to create.
        exist_ok (bool): Already-existing directories treated as successfully created
            if True, raises an exception if False.
        create_parents (bool): Function will create missing parent directories if True,
            will not (and raise an exception) if False.

    Returns:
        str: Path of the created directory.
    """
    try:
        os.makedirs(folder_path) if create_parents else os.mkdir(folder_path)
    except WindowsError as error:
        # [Error 183] Cannot create a file when that file already exists: {path}
        if not (exist_ok and error.winerror == 183):
            raise

    return folder_path


def extract_archive(archive_path, extract_path, password=None):
    """Extract files from archive into the extract path.

    Args:
        archive_path (str): Path of archive file.
        extract_path (str): Path of folder to extract into.
        password (str): Password for any encrypted contents.

    Returns:
        bool: True if archived extracted, False otherwise.
    """
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            archive.extractall(extract_path, pwd=password)
    except zipfile.BadZipfile:
        LOG.warning("%s not a valid archive.", archive_path)
        extracted = False
    else:
        extracted = True
    return extracted


def flattened_path(path, flat_char="_"):
    """Returns "flattened" version of given path, with no separators."""
    for char in [os.sep, ":"]:
        path = path.replace(char, flat_char)
    while flat_char * 2 in path:
        path = path.replace(flat_char * 2, flat_char)
    while path.startswith(flat_char) or path.endswith(flat_char):
        path = path.strip(flat_char)
    return path


def folder_file_paths(folder_path, top_level_only=False, **kwargs):
    """Generate paths for files in folder.

    Args:
        folder_path (str): Path for folder to list file paths within.
        top_level_only (bool): Only yield paths for files at top-level if True; include
            subfolders as well if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        file_extensions (iter): Collection of file extensions to filter files. Include
            the period in the extension: `.ext`. Use empty string "" for files without
            an extension.

    Yields:
        str
    """
    if "file_extensions" in kwargs:
        kwargs["file_extensions"] = {ext.lower() for ext in kwargs["file_extensions"]}
    for i, (_folder_path, _, file_names) in enumerate(os.walk(folder_path)):
        for file_name in file_names:
            ext = os.path.splitext(file_name)[1].lower()
            if "file_extensions" not in kwargs or ext in kwargs["file_extensions"]:
                yield os.path.join(_folder_path, file_name)

        if top_level_only and i == 0:
            return


def folder_relative_file_paths(folder_path, top_level_only=False, **kwargs):
    """Generate paths for files in folder, relative to top-level.

    Args:
        folder_path (str): Path for folder to list file paths within.
        top_level_only (bool): Only yield paths for files at top-level if True; include
            subfolders as well if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        file_extensions (iter): Collection of file extensions to filter files. Include
            the period in the extension: `.ext`. Use empty string "" for files without
            an extension.

    Yields:
        str
    """
    file_paths = folder_file_paths(folder_path, top_level_only, **kwargs)
    for file_path in file_paths:
        yield os.path.relpath(file_path, folder_path)


# Py2.
def same_file(file_path, cmp_file_path, not_exists_ok=True):
    """Determine if given files are the same.

    Args:
        file_path (str): Path to file.
        cmp_file_path (str): Path to comparison file.
        not_exists_ok (bool): True if a path for a nonexistent file will be treated as a
            file and as "different" than any actual files.

    Returns:
        bool
    """
    # Code similar to Py3 version below.
    file_paths = [file_path, cmp_file_path]
    for file_path in file_paths:
        if not_exists_ok and file_path is not None:
            # Check for non-files (folders).
            if os.path.exists(file_path) and not os.path.isfile(file_path):
                raise OSError("`{}` is not a file.".format(file_path))

        elif file_path is None or not os.path.exists(file_path):
            raise OSError(
                "`{}` does not exist (`not_exists_ok=False`).".format(file_path)
            )

    if any(file_path is None for file_path in file_paths):
        non_count = len([file_path for file_path in file_paths if file_path is None])
        same = non_count == len(file_paths)
    elif any(not os.path.exists(file_path) for file_path in file_paths):
        non_count = len(
            [file_path for file_path in file_paths if not os.path.exists(file_path)]
        )
        same = non_count == len(file_paths)
    else:
        same = all(
            filecmp.cmp(file_path, cmp_file_path)
            for file_path, cmp_file_path in pairwise(file_paths)
        )
    return same


# Py3.
# def same_file(*file_paths, not_exists_ok=True):
#     """Determine if given files are the same.

#     Args:
#         *file_paths (iter of str): Collection of paths of files to compare.
#         not_exists_ok (bool): True if a path for a nonexistent file will be treated as
#             a file and as "different" than any actual files.

#     Returns:
#         bool
#     """
#     for file_path in file_paths:
#         if not_exists_ok and file_path is not None:
#             # Check for non-files (folders).
#             if os.path.exists(file_path) and not os.path.isfile(file_path):
#                 raise OSError("`{}` is not a file.".format(file_path))

#         elif file_path is None or not os.path.exists(file_path):
#                 raise OSError(
#                     "`{}`` does not exist (`not_exists_ok=False`).".format(file_path)
#                 )

#     if any(file_path is None for file_path in file_paths):
#         non_count = len([file_path for file_path in file_paths if file_path is None])
#         same = non_count == len(file_paths)
#     elif any(not os.path.exists(file_path) for file_path in file_paths):
#         non_count = len(
#             [file_path for file_path in file_paths if not os.path.exists(file_path)]
#         )
#         same = non_count == len(file_paths)
#     else:
#         same = all(
#             filecmp.cmp(file_path, cmp_file_path)
#             for file_path, cmp_file_path in pairwise(file_paths)
#         )
#     return same


def update_file(file_path, source_path):
    """Update file from source.

    Args:
        file_path (str): Path to file to be updated.
        source_path (str): Path to source file.

    Returns:
        str: Result key--"updated", "failed to update", or "no update necessary".
    """
    if not os.path.isfile(source_path):
        raise OSError("Source path {}` is not a file.".format(source_path))

    if same_file(file_path, source_path):
        result_key = "no update necessary"
    else:
        # Make destination file overwriteable.
        if os.path.exists(file_path):
            os.chmod(file_path, stat.S_IWRITE)
        # Create directory structure (if necessary).
        create_folder(os.path.dirname(file_path), exist_ok=True, create_parents=True)
        try:
            shutil.copy2(source_path, file_path)
        except IOError:
            result_key = "failed to update"
        else:
            os.chmod(file_path, stat.S_IWRITE)
            result_key = "updated"
        level = logging.INFO if result_key == "updated" else logging.WARNING
        LOG.log(level, "%s %s at %s.", source_path, result_key, file_path)
    return result_key


def update_replica_folder(folder_path, source_path, top_level_only=False, **kwargs):
    """Update replica folder from source.

    Args:
        folder_path (str): Path to replica folder.
        source_path (str): Path to source folder.
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
    log = kwargs.get("logger", LOG)
    log.info("Start: Update replica folder `%s` from `%s`.", folder_path, source_path)
    for repo_path in (folder_path, source_path):
        if not os.access(repo_path, os.R_OK):
            raise OSError("Cannot access `{}`.".format(repo_path))

    states = Counter()
    relative_source_paths = folder_relative_file_paths(
        source_path, top_level_only, file_extensions=kwargs["file_extensions"]
    )
    for i, relative_path in enumerate(relative_source_paths, start=1):
        source_file_path = os.path.join(source_path, relative_path)
        if kwargs.get("flatten_tree", False):
            file_path = os.path.join(folder_path, os.path.basename(relative_path))
        else:
            file_path = os.path.join(folder_path, relative_path)
            # Add directory (if necessary).
            create_folder(
                os.path.dirname(file_path), exist_ok=True, create_parents=True
            )
        states[update_file(file_path, source_file_path)] += 1
        if "log_evaluated_division" in kwargs:
            if i % kwargs["log_evaluated_division"] == 0:
                log.info("Evaluated {:,} files.".format(i))
    log_entity_states("files", states, log, log_level=logging.INFO)
    elapsed(start_time, log)
    log.info("End: Update.")
    return states
