"""Communication objects."""
try:
    from collections.abc import Iterable
except ImportError:
    # Py2.
    from collections import Iterable
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
from pathlib import Path
import re
import smtplib
import sys

# Py2.
if sys.version_info.major >= 3:
    basestring = str


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def extract_email_addresses(*sources):
    """Generate email addresses parsed from various source objects.

    Sources can be strings (unicode or bytes) or a container that may have strings in
    them (dict or Iterable). If a source is of an unsupported type, it will be treated
    as an empty container.

    Args:
        *sources: Source objects to examine & extract from.

    Yields:
        str: Email address.
    """
    for element in sources:
        if isinstance(element, basestring):
            addresses = (
                address for address in re.findall(r"[\w\.-]+@[\w\.-]+", element)
            )
        elif isinstance(element, Iterable):
            addresses = extract_email_addresses(*element)
        elif isinstance(element, dict):
            addresses = extract_email_addresses(*element.items())
        # If type is unsupported, just make empty generator. This makes parsing emails
        # from mixed-type collections easier without filtering.
        else:
            addresses = (_ for _ in ())
        for address in addresses:
            yield address


def send_email_smtp(host, from_address, to_addresses, subject, body=None, **kwargs):
    """Send email (via SMTP).

    Args:
        host (str): Host name of SMTP server.
        from_address (str): Email address for sender.
        to_addresses (iter, str): Email addresses for recipient(s).
        subject (str): Message subject line.
        body (str, None): Message body text.
        **kwargs: Arbitrary keyword arguments. See below.

    Kwargs:
        port (int, str): Port to connect to SMTP host on. Default is 25.
        password (str, None): Password for authentication with host. Default is None.
        copy_addresses (list, str): Email addresses for message copy-recipients (cc).
        blind_copy_addresses (list, str): Email addresses for message blind copy-
            recipients (bcc).
        reply_to_addresses (list, str): Email addresses for message reply recipients.
        body_type (str): MIME subtype of body text. Options are "plain" and "html".
            Default is "plain".
        attachment_paths (iter of pathlib.Path or str): Collection of file paths to be
            attached to message.
    """
    to_addresses = list(extract_email_addresses(to_addresses))
    kwargs.setdefault("port", 25)
    kwargs.setdefault("password")
    for prefix in ["copy", "blind_copy", "reply_to"]:
        kwargs[f"{prefix}_addresses"] = list(
            extract_email_addresses(kwargs.get(f"{prefix}_addresses"))
        )
    kwargs.setdefault("body_type", "plain")
    if kwargs.get("attachment_paths"):
        kwargs["attachment_paths"] = [Path(path) for path in kwargs["attachment_paths"]]
    kwargs.setdefault("attachment_paths", [])
    recipient_addresses = (
        to_addresses + kwargs["copy_addresses"] + kwargs["blind_copy_addresses"]
    )
    message = MIMEMultipart()
    message.add_header("From", from_address)
    if to_addresses:
        message.add_header("To", ",".join(to_addresses))
    if kwargs["copy_addresses"]:
        message.add_header("Cc", ",".join(kwargs["copy_addresses"]))
    if kwargs["reply_to_addresses"]:
        message.add_header("Reply-To", ",".join(kwargs["reply_to_addresses"]))
    message.add_header("Subject", subject)
    if body:
        message.attach(MIMEText(body, kwargs["body_type"]))
    for attachment_path in kwargs["attachment_paths"]:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(payload=attachment_path.open(mode="rb").read())
        encode_base64(msg=part)
        part.add_header(
            "Content-Disposition", "attachment", filename=attachment_path.name
        )
        message.attach(payload=part)
    connection = smtplib.SMTP(host=host, port=int(kwargs["port"]))
    connection.starttls()
    # Only bother to log in if password provided (some SMTP hosts authenticate by IP).
    if kwargs["password"]:
        connection.login(user=from_address, password=kwargs["password"])
    try:
        connection.send_message(
            msg=message, from_addr=from_address, to_addrs=recipient_addresses
        )
    except AttributeError:
        # Py2.
        connection.sendmail(
            from_addr=from_address,
            to_addrs=recipient_addresses,
            msg=message.as_string(),
        )
    finally:
        connection.quit()


def send_links_email(host, from_address, to_addresses, subject, urls, **kwargs):
    """Send email with a listing of URLs. Body is HTML by default.

    Args:
        host (str): Host name of SMTP server.
        from_address (str): Email address for sender.
        to_addresses (iter, str): Email addresses for recipient(s).
        subject (str): Message subject line.
        urls (iter): Collection of URLs to link.
        **kwargs: Arbitrary keyword arguments. See below.

    Kwargs:
        ordered (bool): True if link list is ordered, False otherwise. Default is False.
        body_pre_links (str): Message body text to place before the links list.
        body_post_links (str): Message body text to place after the links list.
        See additional kwargs for `proctools.communicate.send_email_smtp`.
    """
    kwargs.setdefault("ordered", False)
    for part in ["pre", "post"]:
        kwargs.setdefault(f"body_{part}_links", "")
    html_list = "".join(f"""<li><a href="{url}">{url}</a></li>""" for url in urls)
    tag = "ol" if kwargs["ordered"] else "ul"
    body = (
        kwargs["body_pre_links"]
        + f"""<{tag}>{html_list}</{tag}>"""
        + kwargs["body_post_links"]
    )
    send_email_smtp(
        host, from_address, to_addresses, subject, body, body_type="html", **kwargs
    )
