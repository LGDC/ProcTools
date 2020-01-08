"""Value-building, -deriving, and -cleaning objects."""
import logging
import string
import sys
import unicodedata

import dateutil.parser

# Py2.
if sys.version_info.major >= 3:
    basestring = str
    unicode = str


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def clean_whitespace(value, clear_empty_string=True):
    """Return value with whitespace stripped & deduplicated.

    Args:
        value (str): Value to alter.
        clear_empty_string (bool): Convert empty string results to NoneTypes if True.

    Returns
        str: Altered value.
    """
    if value is not None:
        value = value.strip()
        for character in string.whitespace:
            while character * 2 in value:
                value = value.replace(character * 2, character)
    if clear_empty_string and not value:
        value = None
    return value


def clean_whitespace_without_clear(value):
    """Return value with whitespace stripped & deduplicated.

    Will not return NoneType if string is (or ends up) empty.

    Args:
        value (str): Value to alter.

    Returns
        str: Altered value.
    """
    return clean_whitespace(value, clear_empty_string=False)


def concatenate(*values, **kwargs):
    """Return concatenated string from ordered values with separator.

    Ignores NoneTypes.

    Args:
        *values: Variable length argument list.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        separator (str): Character(s) to separate values with. Default is a single
            space " ".

    Returns:
        str: Concatenated values.
    """
    separator = kwargs.get("separator", " ")
    return separator.join(str(value).strip() for value in values if value is not None)


def datetime_from_string(value):
    """Return datetime object from input if possible, None if not.

    Args:
        value (str): Value to alter.

    Returns:
        datetime: Extracted value.
    """
    try:
        result = dateutil.parser.parse(value) if value else None
    except ValueError:
        result = None
    return result


def force_case(value, case_method_name):
    """Return value converted to chosen case style.

    Args:
        value (str): Value to alter.
        case_method_name (str): Name of the method that converts string to case.

    Returns:
        str: Altered value.
    """
    if value:
        value = getattr(value, case_method_name.lower())()
    return value


def force_lowercase(value):
    """Return value converted to lowercase.

    Args:
        value (str): Value to alter.

    Returns:
        str: Altered value.
    """
    return force_case(value, "lower")


def force_title_case(value):
    """Return value converted to title case.

    Args:
        value (str): Value to alter.

    Returns:
        str: Altered value.
    """
    return force_case(value, "title")


def force_uppercase(value):
    """Return value converted to uppercase.

    Args:
        value (str): Value to alter.

    Returns:
        str: Altered value.
    """
    return force_case(value, "upper")


def force_yn(value, default=None):
    """Return given value if valid "Y" or "N" representation; otherwise return default.

    Args:
        value (str): Value to alter.
        default (str): String to force if value not valid.

    Returns:
        str: Altered value.
    """
    return value if value in ("n", "N", "y", "Y") else default


def is_numeric(value, nonetype_ok=True):
    """Return True if value is numeric.

    Props to: http://pythoncentral.io/
        how-to-check-if-a-string-is-a-number-in-python-including-unicode/

    Args:
        value (str): Value to evaluate.
        nonetype_ok (bool): True if NoneType value evaluated as a True result, False
            otherwise.

    Returns:
        bool: True if numeric, False otherwise.
    """
    try:
        float(value)
    except TypeError:
        if value is None and nonetype_ok:
            result = True
        else:
            LOG.exception("Value %r type handling not defined.", value)
            raise

    except ValueError:
        result = False
    else:
        result = str(value).lower() not in ("nan", "inf")
    return result


def parity(*numbers):
    """Return proper parity description for a collection of integers.

    Parity description can be: "even", "odd", or "mixed".

    Args:
        *numbers: Collection of numbers.

    Returns:
        str
    """
    numbers_bitwise = {n & 1 for n in numbers}
    if not numbers_bitwise:
        result = None
    elif len(numbers_bitwise) == 1:
        result = {0: "even", 1: "odd"}[numbers_bitwise.pop()]
    else:
        result = "mixed"
    return result


def remove_diacritics(value):
    """Return string with diacritics removed.

    Args:
        value (str): Value to alter.

    Returns:
        str: Altered value.
    """
    if value:
        value = u"".join(
            char
            for char in unicodedata.normalize("NFKD", value)
            if not unicodedata.combining(char)
        )
    return value


def same_string_casefold(*values):
    """Return True if strings are same, normalized & ignoring case.

    Args:
        *values (iter): Collection of values to check.

    Returns:
        bool: True if strings are same, False otherwise.
    """
    if len(values) <= 1:
        return True

    if all(val is None for val in values):
        return True

    if any(not isinstance(string, basestring) for string in values):
        return False

    cmp_values = set()
    for value in values:
        # Force text to same case/normal unicode state for comparison.
        try:
            value = unicode(value).casefold()
        except AttributeError:
            value = unicode(value).upper().lower()
        cmp_values.add(unicodedata.normalize("NFKD", value))
    return len(cmp_values) == 1
