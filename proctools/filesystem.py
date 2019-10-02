"""File system objects."""
try:
    from contextlib import ContextDecorator
except ImportError:
    # Py2.
    from contextlib2 import ContextDecorator
import logging
import os
import subprocess
import zipfile


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


def archive_directory(directory_path, archive_path, directory_as_base=False, **kwargs):
    """Create zip archive of files in the given directory.

    The exclude pattern will ignore any directory or file name that includes any
        pattern listed.

    Args:
        directory_path (str): Path of directory to archive.
        archive_path (str): Path of archive to create.
        directory_as_base (bool): Place contents in the base directory within the
            archive if True, do not if False.


    Keyword Args:
        archive_exclude_patterns (iter): Collection of file name patterns to exclude
            from archive.
        encrypt_password (str): Password for an encrypted wrapper archive to place the
            directory archive inside. Default is None (no encryption/wrapper).

    Returns:
        str: Path of archive created.
    """
    kwargs.setdefault("archive_exclude_patterns", [])
    kwargs.setdefault("encrypt_password")
    LOG.info("Start: Create archive of directory %s.", directory_path)
    if directory_as_base:
        directory_root_length = len(os.path.dirname(directory_path)) + 1
    else:
        directory_root_length = len(directory_path) + 1
    archive = zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED)
    with archive:
        for subdirectory_path, _, file_names in os.walk(directory_path):
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


def create_directory(directory_path, exist_ok=False, create_parents=False):
    """Create directory at given path.

    Args:
        directory_path (str): Path of directory to create.
        exist_ok (bool): Already-existing directories treated as successfully created
            if True, raises an exception if False.
        create_parents (bool): Function will create missing parent directories if True,
            Will not (and raise an exception) if False.

    Returns:
        str: Path of the created directory.
    """
    try:
        os.makedirs(directory_path) if create_parents else os.mkdir(directory_path)
    except WindowsError as error:
        # [Error 183] Cannot create a file when that file already exists: {path}
        if not (exist_ok and error.winerror == 183):
            raise

    return directory_path


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
    """
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            yield file_path
