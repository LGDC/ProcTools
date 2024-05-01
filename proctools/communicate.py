"""Communication objects."""
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging import Logger, getLogger
from pathlib import Path
from re import findall
from smtplib import SMTP
from typing import Iterable, Iterator, Optional, Union


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""


def extract_email_addresses(
    *sources: Union[str, bytes, dict, Iterable]
) -> Iterator[str]:
    """Generate email addresses parsed from various source objects.

    Sources can be strings (unicode or bytes) or a container that may have strings in
    them. If a source is of an unsupported type, it will be treated as an empty
    container.

    Args:
        *sources: Source objects to examine & extract from.

    Yields:
        Email address.
    """
    for source in sources:
        # Assume bytes are encoded UTF-8.
        if isinstance(source, bytes):
            source = source.decode("utf-8")
        if isinstance(source, str):
            yield from findall(r"[\w\.-]+@[\w\.-]+", source)

        elif isinstance(source, Iterable):
            yield from extract_email_addresses(*source)

        elif isinstance(source, dict):
            yield from extract_email_addresses(*source.items())

        # If type is unsupported, just make empty generator. This makes parsing emails
        # from mixed-type collections easier without filtering.
        else:
            yield from ()


def send_email_smtp(
    *,
    from_address: str,
    to_addresses: Optional[Union[Iterable[str], str]] = None,
    copy_addresses: Optional[Union[Iterable[str], str]] = None,
    blind_copy_addresses: Optional[Union[Iterable[str], str]] = None,
    reply_to_addresses: Optional[Union[Iterable[str], str]] = None,
    subject: Optional[str] = None,
    body: Optional[str] = None,
    body_type: str = "plain",
    attachment_paths: Optional[Iterable[Union[Path, str]]] = None,
    host: str,
    port: int = 25,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> None:
    """Send email via SMTP.

    Args:
        from_address: Email address for sender.
        to_addresses: Email addresses for recipients.
        copy_addresses: Email addresses for message copy-recipients (cc).
        blind_copy_addresses: Email addresses for message blind copy-recipients (bcc).
        reply_to_addresses: Email addresses for message reply recipients.
        subject: Message subject line.
        body: Message body text.
        body_type: MIME subtype of body text. Options are "plain" and "html".
        attachment_paths: Collection of paths for files to attach to message.
        host: Host name of SMTP server.
        port: Port to connect to SMTP host on.
        username: Username for authentication with host.
        password: Password for authentication with host.
    """
    message = MIMEMultipart()
    message.add_header("From", from_address)
    recipient_addresses = []
    if to_addresses:
        to_addresses = list(extract_email_addresses(to_addresses))
        message.add_header("To", ",".join(to_addresses))
        recipient_addresses.extend(to_addresses)
    if copy_addresses:
        copy_addresses = list(extract_email_addresses(copy_addresses))
        message.add_header("Cc", ",".join(copy_addresses))
        recipient_addresses.extend(copy_addresses)
    if blind_copy_addresses:
        blind_copy_addresses = list(extract_email_addresses(blind_copy_addresses))
        recipient_addresses.extend(blind_copy_addresses)
    if reply_to_addresses:
        message.add_header(
           "Reply-To", ",".join(extract_email_addresses(reply_to_addresses))
        )
    if subject:
        message.add_header("Subject", subject)
    if body:
        message.attach(MIMEText(body, body_type))
    if attachment_paths:
        for attachment_path in attachment_paths:
            attachment_path = Path(attachment_path)
            part = MIMEBase("application", "octet-stream")
            part.set_payload(payload=attachment_path.open(mode="rb").read())
            encode_base64(msg=part)
            part.add_header(
                "Content-Disposition", "attachment", filename=attachment_path.name
            )
            message.attach(payload=part)
    connection = SMTP(host=host, port=port)
    connection.starttls()
    # Only bother to log in if username & password provided
    # (some SMTP hosts authenticate by IP).
    if username and password:
        connection.login(user=username, password=password)
    connection.send_message(
        msg=message, from_addr=from_address, to_addrs=recipient_addresses
    )
    connection.quit()


def send_links_email(
    *,
    from_address: str,
    to_addresses: Optional[Union[Iterable[str], str]] = None,
    copy_addresses: Optional[Union[Iterable[str], str]] = None,
    blind_copy_addresses: Optional[Union[Iterable[str], str]] = None,
    reply_to_addresses: Optional[Union[Iterable[str], str]] = None,
    subject: Optional[str] = None,
    link_urls: Iterable[str],
    order_link_list: bool = False,
    body_before_links: Optional[str] = None,
    body_after_links: Optional[str] = None,
    attachment_paths: Optional[Iterable[Union[Path, str]]] = None,
    host: str,
    port: int = 25,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> None:
    """Send email with a listing of URLs via SMTP.

    Args:
        from_address: Email address for sender.
        to_addresses: Email addresses for recipients.
        copy_addresses: Email addresses for message copy-recipients (cc).
        blind_copy_addresses: Email addresses for message blind copy-recipients (bcc).
        reply_to_addresses: Email addresses for message reply recipients.
        subject: Message subject line.
        link_urls: Sequence of URLs to list as links.
        order_link_list: Make link list ordered if True.
        body_before_links: Message body HTML to place before link list.
        body_after_links: Message body HTML to place after link list.
        attachment_paths: Collection of paths for files to attach to message.
        host: Host name of SMTP server.
        port: Port to connect to SMTP host on.
        username: Username for authentication with host.
        password: Password for authentication with host.
    """
    # for part in ["pre", "post"]:
    #     kwargs.setdefault(f"body_{part}_links", "")
    links = [f"""<li><a href="{url}">{url}</a></li>""" for url in link_urls]
    list_tag = "ol" if order_link_list else "ul"
    body = f"""<{list_tag}>{"".join(links)}</{list_tag}>"""
    if body_before_links:
        body = body_before_links + body
    if body_after_links:
        body = body + body_after_links
    send_email_smtp(
        from_address=from_address,
        to_addresses=to_addresses,
        copy_addresses=copy_addresses,
        blind_copy_addresses=blind_copy_addresses,
        reply_to_addresses=reply_to_addresses,
        subject=subject,
        body=body,
        body_type="html",
        attachment_paths=attachment_paths,
        host=host,
        port=port,
        username=username,
        password=password,
    )
