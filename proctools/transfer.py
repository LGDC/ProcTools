"""Data & file transfer objects."""
import datetime
import ftplib
import logging
from pathlib import Path, PurePosixPath
import time

import dropbox
import pysftp


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def dropbox_get_share_link(share_path, app_token, **kwargs):
    """Return shareable URL for Dropbox file or folder.

    Note: Basic user accounts cannot set a link password or expiration date/time.

    Args:
        share_path (pathlib.PurePosixPath, str): POSIX path relative to Dropbox (app)
            root folder for file/folder to share.
        app_token (str): Registered app token for API access.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        link_password (str, None): Password to access the link. Default is None (no
            password).
        link_expires (datetime.datetime, None): Date & time the link will expire.
            Default is None (never expires).

    Returns:
        str
    """
    share_path = PurePosixPath(share_path)
    # Dropbox path requires explicit path from app-root.
    if not bool(share_path.root):
        share_path = PurePosixPath("/", share_path)
    kwargs.setdefault("link_password")
    kwargs.setdefault("link_expires")
    api = dropbox.Dropbox(oauth2_access_token=app_token)
    settings = dropbox.sharing.SharedLinkSettings(
        link_password=kwargs["link_password"],
        requested_visibility=(
            dropbox.sharing.RequestedVisibility.password
            if kwargs["link_password"]
            else dropbox.sharing.RequestedVisibility.public
        ),
        expires=kwargs["link_expires"],
    )
    link_meta = api.sharing_create_shared_link_with_settings(
        # dropbox v10.10.0: Convert to str.
        path=str(share_path),
        settings=settings,
    )
    return link_meta.url.replace("?dl=0", "?dl=1")


def dropbox_upload_file(source_path, destination_path, app_token, **kwargs):
    """Upload file to Dropbox.

    Args:
        source_path (pathlib.Path, str): Path of file to upload.
        destination_path (pathlib.PurePosixPath, str): POSIX path relative to Dropbox
            (app) root folder to upload into.
        app_token (str): Registered app token for API access.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        chunk_size (int): Size of individual upload chunks within the session. Default
            is whichever is smaller of file size or 128 MB (decimal).

    Returns:
        pathlib.PurePosixPath: POSIX path in Dropbox (app).
    """
    source_path = Path(source_path)
    destination_path = PurePosixPath(destination_path)
    # Dropbox path requires explicit path from app-root.
    if not bool(destination_path.root):
        destination_path = PurePosixPath("/", destination_path)
    file_size = source_path.stat().st_size
    kwargs.setdefault("chunk_size", min(file_size + 1, 134_217_728))
    LOG.info("Uploading %s to Dropbox.", source_path)
    commit_kwargs = {
        # dropbox v10.10.0: Convert to str.
        "path": str(destination_path),
        "mode": dropbox.files.WriteMode("overwrite"),
        "client_modified": datetime.datetime(
            *time.gmtime(source_path.stat().st_mtime)[:6]
        ),
        "mute": True,
    }
    api = dropbox.Dropbox(oauth2_access_token=app_token)
    stream = source_path.open(mode="rb")
    if file_size <= kwargs["chunk_size"]:
        file_meta = api.files_upload(f=stream.read(), **commit_kwargs)
    else:
        session_meta = api.files_upload_session_start(
            f=stream.read(kwargs["chunk_size"])
        )
        cursor = dropbox.files.UploadSessionCursor(
            session_id=session_meta.session_id, offset=stream.tell()
        )
        while (file_size - stream.tell()) > kwargs["chunk_size"]:
            api.files_upload_session_append_v2(
                f=stream.read(kwargs["chunk_size"]), cursor=cursor
            )
            cursor.offset = stream.tell()
        file_meta = api.files_upload_session_finish(
            f=stream.read(kwargs["chunk_size"]),
            cursor=cursor,
            commit=dropbox.files.CommitInfo(**commit_kwargs),
        )
    return PurePosixPath(file_meta.path_display)


def ftp_upload_file(source_path, destination_path, host, **kwargs):
    """Upload file to FTP site.

    Args:
        source_path (pathlib.Path, str): Path of file to upload.
        destination_path (pathlib.PurePosixPath, str): POSIX path relative to FTP root
            folder to upload into.
        host (str): Host name of FTP site.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        port (int): Port to connect to. Default is 21.
        username (str, None): Credential user name. Default is None (no credentials).
        password (str, None): Credential password. Default is None (no credentials).

    Returns:
        pathlib.PurePosixPath: POSIX path from FTP where file was placed at.
    """
    source_path = Path(source_path)
    destination_path = PurePosixPath(destination_path)
    kwargs.setdefault("port", 21)
    kwargs.setdefault("username")
    kwargs.setdefault("password")
    LOG.info("Uploading `%s` to FTP site at %s.", source_path, host)
    try:
        ftp = ftplib.FTP(host="")
        ftp.connect(host, port=kwargs["port"])
        ftp.login(user=kwargs["username"], passwd=kwargs["password"])
        # Py 3.7.10: Convert to str.
        ftp.cwd(str(destination_path.parent))
        with source_path.open(mode="rb") as file:
            ftp.storbinary(cmd=f"STOR {destination_path.name}", fp=file)
    finally:
        ftp.quit()
    LOG.info("`%s` uploaded to `%s%s`.", source_path, host, destination_path)
    return destination_path


def secure_ftp_upload_file(source_path, destination_path, host, **kwargs):
    """Upload files to Secure FTP site.

    Args:
        source_path (pathlib.Path, str): Path of file to upload.
        destination_path (pathlib.PurePosixPath, str): POSIX path relative to FTP root
            folder to upload into.
        host (str): Host name of FTP site.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        port (int): Port to connect to on host. Default is 22.
        username (str, None): Credential user name. Default is None (no credentials).
        password (str, None): Credential password. Default is None (no credentials).
        private_key (pathlib.Path, str, None): Path to keyfile, or key string. Default
            is None (no private key).

    Returns:
        pathlib.PurePosixPath: POSIX path from FTP where file was placed at.
    """
    source_path = Path(source_path)
    destination_path = PurePosixPath(destination_path)
    kwargs.setdefault("port", 22)
    kwargs.setdefault("username")
    kwargs.setdefault("password")
    kwargs.setdefault("private_key")
    LOG.info("Uploading `%s` to Secure FTP site at %s.", source_path, host)
    connection_options = pysftp.CnOpts()
    # Yeah, this is not great, but not that worried about MitM in our cases.
    connection_options.hostkeys = None
    sftp = pysftp.Connection(host, cnopts=connection_options, **kwargs)
    with sftp:
        sftp.put(
            localpath=source_path, remotepath=destination_path, preserve_mtime=True
        )
    LOG.info("`%s` uploaded to `%s`.", source_path, host + destination_path)
    return destination_path
