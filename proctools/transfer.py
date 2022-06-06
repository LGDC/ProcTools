"""Data & file transfer objects."""
from datetime import datetime
from ftplib import FTP
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from time import gmtime
from typing import Optional, Union

import dropbox
import pysftp


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""


def dropbox_get_share_link(
    share_path: Union[PurePosixPath, str],
    *,
    app_token: str,
    link_password: Optional[str] = None,
    time_link_expires: Optional[datetime] = None,
) -> str:
    """Return shareable URL for Dropbox file or folder.

    Note: Basic user accounts cannot set a link password or expiration date/time.

    Args:
        share_path: POSIX path to item to share, relative to Dropbox (app) root folder.
        app_token: Registered app token for API access.
        link_password: Password for accessing link. If set to None, no password set.
        time_link_expires: Time when link will expire. If set to None, never expires.
    """
    share_path = PurePosixPath(share_path)
    # Dropbox path requires explicit path from app-root.
    if not bool(share_path.root):
        share_path = PurePosixPath("/", share_path)
    api = dropbox.Dropbox(oauth2_access_token=app_token)
    settings = dropbox.sharing.SharedLinkSettings(
        link_password=link_password,
        requested_visibility=(
            dropbox.sharing.RequestedVisibility.password
            if link_password
            else dropbox.sharing.RequestedVisibility.public
        ),
        expires=time_link_expires,
    )
    # dropbox v10.10.0: Convert PurPosixPath to str.
    link_meta = api.sharing_create_shared_link_with_settings(
        path=str(share_path), settings=settings
    )
    return link_meta.url.replace("?dl=0", "?dl=1")


def dropbox_upload_file(
    source_path: Union[Path, str],
    *,
    destination_path: Union[PurePosixPath, str],
    app_token: str,
    chunk_size: Optional[int] = None,
) -> PurePosixPath:
    """Upload file to Dropbox.

    Args:
        source_path: Path to source file to upload.
        destination_path: POSIX path to file destination, relative to Dropbox (app) root
            folder.
        share_path: POSIX path to item to share, relative to Dropbox (app) root folder.
        app_token: Registered app token for API access.
        chunk_size: Size of individual upload chunks within the session. If set to None,
            will be the smaller of file size or 128 MB.

    Returns:
        POSIX path to file in Dropbox (app).
    """
    source_path = Path(source_path)
    destination_path = PurePosixPath(destination_path)
    # Dropbox path requires explicit path from app-root.
    if not bool(destination_path.root):
        destination_path = PurePosixPath("/", destination_path)
    file_size = source_path.stat().st_size
    if not chunk_size:
        chunk_size = min(file_size + 1, 134_217_728)
    commit_kwargs = {
        # dropbox v10.10.0: Convert PurePosixPath to str.
        "path": str(destination_path),
        "mode": dropbox.files.WriteMode("overwrite"),
        "client_modified": datetime(*gmtime(source_path.stat().st_mtime)[:6]),
        "mute": True,
    }
    api = dropbox.Dropbox(oauth2_access_token=app_token)
    stream = source_path.open(mode="rb")
    if file_size <= chunk_size:
        file_meta = api.files_upload(f=stream.read(), **commit_kwargs)
    else:
        session_meta = api.files_upload_session_start(f=stream.read(chunk_size))
        cursor = dropbox.files.UploadSessionCursor(
            session_id=session_meta.session_id, offset=stream.tell()
        )
        while (file_size - stream.tell()) > chunk_size:
            api.files_upload_session_append_v2(f=stream.read(chunk_size), cursor=cursor)
            cursor.offset = stream.tell()
        file_meta = api.files_upload_session_finish(
            f=stream.read(chunk_size),
            cursor=cursor,
            commit=dropbox.files.CommitInfo(**commit_kwargs),
        )
    return PurePosixPath(file_meta.path_display)


def ftp_upload_file(
    source_path: Union[Path, str],
    *,
    destination_path: Union[PurePosixPath, str],
    host: str,
    port: int = 21,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> PurePosixPath:
    """Upload file to FTP site.

    Args:
        source_path: Path to source file to upload.
        destination_path: POSIX path to file destination, relative to site root folder.
        host: Host name of FTP site.
        port: Port to connect to site on.
        username: Name of user for authentication with site. If set to None, will not
            authenticate with credentials.
        password: Password for authentication with site. If set to None, will not
            authenticate with credentials.

    Returns:
        POSIX path to file on site.
    """
    source_path = Path(source_path)
    destination_path = PurePosixPath(destination_path)
    try:
        ftp = FTP(host="")
        ftp.connect(host, port=port)
        ftp.login(user=username, passwd=password)
        # Py 3.7.10: Convert PurePosixPath to str.
        ftp.cwd(str(destination_path.parent))
        with source_path.open(mode="rb") as file:
            ftp.storbinary(cmd=f"STOR {destination_path.name}", fp=file)
    finally:
        ftp.quit()
    return destination_path


def secure_ftp_upload_file(
    source_path: Union[Path, str],
    *,
    destination_path: Union[PurePosixPath, str],
    host: str,
    port: int = 22,
    username: Optional[str] = None,
    password: Optional[str] = None,
    private_key: Optional[Union[Path, str]] = None,
) -> PurePosixPath:
    """Upload files to secure FTP site.

    Args:
        source_path: Path to source file to upload.
        destination_path: POSIX path to file destination, relative to site root folder.
        host: Host name of FTP site.
        port: Port to connect to site on.
        username: Name of user for authentication with site. If set to None, will not
            authenticate with credentials.
        password: Password for authentication with site. If set to None, will not
            authenticate with credentials.
        private_key: Path to keyfile, or key string.

    Returns:
        POSIX path to file on site.
    """
    source_path = Path(source_path)
    destination_path = PurePosixPath(destination_path)
    connection_options = pysftp.CnOpts()
    # Yeah, this is not great, but not that worried about MitM in our cases.
    connection_options.hostkeys = None
    sftp = pysftp.Connection(
        host,
        username=username,
        password=password,
        private_key=private_key,
        port=port,
        cnopts=connection_options,
    )
    with sftp:
        sftp.put(
            localpath=source_path, remotepath=destination_path, preserve_mtime=True
        )
    return destination_path
