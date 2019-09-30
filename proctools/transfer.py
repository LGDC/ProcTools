"""Data & file transfer objects."""
import datetime
import ftplib
import logging
import os
import time

import dropbox
import pysftp


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def _scrub_root_path(root_path):
    """Return scrubbed path relative to (UNIX) root is correctly-formed.

    Args:
        root_path (str): Path to scrub.

    Returns:
        str
    """
    # Forward-slash separators.
    root_path = root_path.replace("\\", "/")
    # Dropbox path requires explicit path from app-root; add slash if missing.
    if not root_path.startswith("/"):
        root_path = "/" + root_path
    return root_path


def dropbox_get_share_link(share_path, app_token, **kwargs):
    """Return shareable URL for Dropbox file or folder.

    Note: Basic user accounts cannot set a link password or expiration date/time.

    Args:
        share_path (str): Path from Dropbox (app) root folder of file/folder to share.
        app_token (str): Registered app token for API access.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        link_password (str): Password to access the link. Default is None (no password).
        link_expires (datetime.datetime): Date & time the link will expire. Default is
            None (never expires).

    Returns:
        str
    """
    api = dropbox.Dropbox(app_token)
    settings = dropbox.sharing.SharedLinkSettings(
        requested_visibility=(
            dropbox.sharing.RequestedVisibility.password
            if kwargs.get("link_password")
            else dropbox.sharing.RequestedVisibility.public
        ),
        link_password=kwargs.get("link_password"),
        expires=kwargs.get("link_expires"),
    )
    link_meta = api.sharing_create_shared_link_with_settings(
        path=_scrub_root_path(share_path), settings=settings
    )
    return link_meta.url.replace("?dl=0", "?dl=1")


def dropbox_upload_file(source_path, destination_path, app_token, **kwargs):
    """Upload file to Dropbox.

    Args:
        source_path (str): Path of file to upload.
        destination_path (str): Path from Dropbox (app) root folder to upload into.
        app_token (str): Registered app token for API access.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        chunk_size (int): Size of individual upload chunks within the session. Default
            is whichever is smaller of file size or 128 MB (decimal).

    Returns:
        str: Path from Dropbox (app) root folder where upload was placed at.
    """
    LOG.info("Uploading %s to Dropbox.", source_path)
    file_size = os.path.getsize(source_path)
    kwargs.setdefault("chunk_size", min(file_size + 1, 134217728))
    commit_kwargs = {
        "path": _scrub_root_path(destination_path),
        "mode": dropbox.files.WriteMode("overwrite"),
        "client_modified": datetime.datetime(
            *time.gmtime(os.path.getmtime(source_path))[:6]
        ),
        "mute": True,
    }
    api = dropbox.Dropbox(app_token)
    stream = open(source_path, "rb")
    if file_size <= kwargs["chunk_size"]:
        file_meta = api.files_upload(stream.read(), **commit_kwargs)
    else:
        session_meta = api.files_upload_session_start(stream.read(kwargs["chunk_size"]))
        cursor = dropbox.files.UploadSessionCursor(
            session_id=session_meta.session_id, offset=stream.tell()
        )
        while (file_size - stream.tell()) > kwargs["chunk_size"]:
            api.files_upload_session_append_v2(
                stream.read(kwargs["chunk_size"]), cursor=cursor
            )
            cursor.offset = stream.tell()
        file_meta = api.files_upload_session_finish(
            stream.read(kwargs["chunk_size"]),
            cursor=cursor,
            commit=dropbox.files.CommitInfo(**commit_kwargs),
        )
    return file_meta.path_display


def ftp_upload_file(source_path, destination_path, host, **kwargs):
    """Upload file to FTP site.

    Args:
        source_path (str): Path of file to upload.
        destination_path (str): Path from FTP root folder to upload into.
        host (str): Host name of FTP site.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        username (str): Credential user name. Default is None.
        password (str): Credential password. Default is None.

    Returns:
        str: Path from FTP root folder where file was placed at.
    """
    LOG.info("Uploading %s to FTP site at %s.", source_path, host)
    destination_path = _scrub_root_path(destination_path)
    try:
        ftp = ftplib.FTP(
            host, user=kwargs.get("username"), passwd=kwargs.get("password")
        )
        ftp.cwd(os.path.dirname(destination_path))
        with open(source_path, mode="rb") as file:
            ftp.storbinary("STOR " + os.path.basename(destination_path), file)
    finally:
        ftp.quit()
    LOG.info("%s uploaded to %s.", source_path, host + destination_path)
    return destination_path


def secure_ftp_upload_file(source_path, destination_path, host, **kwargs):
    """Upload files to Secure FTP site.

    Args:
        source_path (str): Path of file to upload.
        destination_path (str): Path from FTP root folder to upload into.
        host (str): Host name of FTP site.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        port (int): Port to connect to on host.
        username (str): Credential user name. Default is None.
        password (str): Credential password. Default is None.
        private_key (str, bytes): Path to keyfile, or key string/bytes. Default is None.

    Returns:
        str: Path from FTP root folder where file was placed at.
    """
    LOG.info("Uploading %s to Secure FTP site at %s.", source_path, host)
    kwargs.setdefault("port", 22)
    destination_path = _scrub_root_path(destination_path)
    connection_options = pysftp.CnOpts()
    # Yeah, this is not great, but not that worried about MitM at the moment.
    connection_options.hostkeys = None
    sftp = pysftp.Connection(host, cnopts=connection_options, **kwargs)
    with sftp:
        sftp.put(
            localpath=source_path, remotepath=destination_path, preserve_mtime=True
        )
    LOG.info("%s uploaded to %s.", source_path, host + destination_path)
    return destination_path
