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
import os
import re
import smtplib
import sys

# Py2.
if sys.version_info.major >= 3:
    basestring = str


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
            addresses = (a for a in re.findall(r"[\w\.-]+@[\w\.-]+", element))
        elif isinstance(element, Iterable):
            addresses = extract_email_addresses(*element)
        elif isinstance(element, dict):
            addresses = extract_email_addresses(*element.items())
        # If type is unsupported, just make empty generator. This makes parsing emails
        #  from mixed-type collections easier without filtering.
        else:
            addresses = (_ for _ in ())
        for address in addresses:
            yield address


def send_email_smtp(
    host, from_address, to_addresses=None, subject=None, body=None, **kwargs
):
    """Send email (via SMTP).

    Args:
        host (str, None): Host name of SMTP server.
        from_address (str): Email address for sender.
        to_addresses (iter, str, None): Email addresses for recipient(s).
        subject (str): Message subject line.
        body (str): Message body text.
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
        attachments (iter of str): Collection of file paths to be attached to message.
    """
    addresses = {
        "to": list(extract_email_addresses(to_addresses)),
        "copy": list(extract_email_addresses(kwargs.get("copy_addresses"))),
        "blind_copy": list(extract_email_addresses(kwargs.get("blind_copy_addresses"))),
        "reply_to": list(extract_email_addresses(kwargs.get("reply_to_addresses"))),
    }
    addresses["all_to"] = addresses["to"] + addresses["copy"] + addresses["blind_copy"]
    if not addresses["all_to"]:
        LOG.info("No to-addresses of any kind--not sending message.")
        return

    message = MIMEMultipart()
    message.add_header("From", from_address)
    if addresses["to"]:
        message.add_header("To", ",".join(addresses["to"]))
    if addresses["copy"]:
        message.add_header("Cc", ",".join(addresses["copy"]))
    if kwargs.get("reply_to_addresses"):
        message.add_header("Reply-To", ",".join(addresses["reply_to"]))
    if subject:
        message.add_header("Subject", subject)
    if body:
        message.attach(MIMEText(body, kwargs.get("body_type", "plain")))
    for attachment in kwargs.get("attachments", []):
        part = MIMEBase("application", "octet-stream")
        part.set_payload(open(attachment, "rb").read())
        encode_base64(part)
        part.add_header(
            "Content-Disposition", "attachment", filename=os.path.basename(attachment)
        )
        message.attach(part)
    connection = smtplib.SMTP(host=host, port=int(kwargs.get("port", 25)))
    connection.starttls()
    # Only bother to log in if password provided (some SMTP hosts authenticate by IP).
    if kwargs.get("password"):
        connection.login(user=from_address, password=kwargs["password"])
    try:
        connection.send_message(
            msg=message, from_addr=from_address, to_addrs=addresses["all_to"]
        )
    except AttributeError:
        # Py2.
        connection.sendmail(
            from_addr=from_address,
            to_addrs=addresses["all_to"],
            msg=message.as_string(),
        )
    connection.quit()


def send_links_email(
    host, from_address, to_addresses=None, subject=None, urls=(), **kwargs
):
    """Send email with a listing of URLs. Body is HTML by default.

    Args:
        host (str, None): Host name of SMTP server.
        from_address (str): Email address for sender.
        to_addresses (iter, str, None): Email addresses for recipient(s).
        subject (str): Message subject line.
        urls (iter): Collection of URLs to link.
        **kwargs: Arbitrary keyword arguments. See below.

    Kwargs:
        ordered (bool): True if link list is ordered, False otherwise. Default is False.
        body_pre_links (str): Message body text to place before the links list.
        body_post_links (str): Message body text to place after the links list.
        See additional kwargs for `proctools.communicate.send_email_smtp`.
    """
    list_item_template = """<li><a href="{0}">{0}</a></li>"""
    list_items = [list_item_template.format(url) for url in urls]
    body = kwargs.get("body_pre_links", "")
    if kwargs.get("ordered", False):
        body += "<ol>{}</ol>".format("".join(list_items))
    else:
        body += "<ul>{}</ul>".format("".join(list_items))
    body += kwargs.get("body_post_links", "")
    send_email_smtp(
        host, from_address, to_addresses, subject, body, body_type="html", **kwargs
    )
