"""Credential store access objects.

The current credential store is a configuration file in the resources
subfolder. Though this seems like bad practice, the file has been set up to
extremely limit access; also the resources folder is ignored by the
repository.
"""
try:
    from contextlib import ContextDecorator
except ImportError:
    # Py2.
    from contextlib2 import ContextDecorator
import logging
import subprocess


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


class UNCPathCredential(ContextDecorator):
    """Simple manager for UNC credentials.

    Attributes:
        path (str): Path to UNC share.
        username (str): Credential user name.
    """

    def __init__(self, unc_path, username=None, password=None):
        """Initialize CredentialUNC instance.

        Args:
            unc_path (str): Path to the UNC share.
            username (str): Credential user name.
            password (str): Credential password.
        """
        self.path = unc_path
        self.username = username
        self.__password = password

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()

    def __str__(self):
        return self.path

    def connect(self, username=None, password=None):
        """Connects the UNC directory.

        Args:
            username (str): Credential user name.
            password (str): Credential password.
        """
        LOG.info("Connecting UNC path %s.", self.path)
        call_string = """net use "{}\"""".format(self.path)
        if password or self.__password:
            call_string += " {}".format(password if password else self.__password)
        if username or self.username:
            call_string += """ /user:"{}\"""".format(
                username if username else self.username
            )
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
