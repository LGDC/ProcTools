"""Value-building, -deriving, and -cleaning objects."""
import datetime
from hashlib import sha256
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


def any_in_range(numbers, floor, ceiling):
    """Return True if any of the integers are in given range.

    Args:
        numbers (iter): Integers to evaluate.
        floor (int): Lowest integer in range.
        ceiling (int): Highest integer in range.

    Returns:
        bool
    """
    return any(number in range(floor, ceiling + 1) for number in numbers)


def clean_whitespace(value, clear_empty_string=True):
    """Return value with whitespace stripped & deduplicated.

    Args:
        value (str): Value to alter.
        clear_empty_string (bool): Convert empty string results to NoneTypes if True.

    Returns
        str: Altered value.
    """
    if isinstance(value, basestring):
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
        wrappers (Collection of str): Two string values with which to place before and
            after each value. For example, ("[", "]") will wrap values with square
            brackets. Default is ("", "") - no wrapper.
        nonetype_replacement (str): Value to replace NoneTypes with (if provided).

    Returns:
        str: Concatenated values.
    """
    separator = kwargs.get("separator", " ")
    wrappers = kwargs.get("wrappers", ["", ""])
    values_to_concatenate = []
    for value in values:
        if value is None:
            if "nonetype_replacement" in kwargs:
                values_to_concatenate.append(kwargs["nonetype_replacement"])
        else:
            values_to_concatenate.append(str(value).strip())
    concatenated = separator.join(
        value.join(wrappers) for value in values_to_concatenate
    )
    return concatenated if concatenated else None


def date_as_datetime(value):
    """Return date or datetime value zero-time datetime.

    Args:
        value (datetime.date, datetime.datetime): Value to alter.

    Returns:
        datetime.datetime
    """
    if isinstance(value, datetime.datetime):
        value = datetime.datetime.combine(value.date(), datetime.datetime.min.time())
    if value:
        value = datetime.datetime(value.year, value.month, value.day)
    return value


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
        if "_" in value:
            result = datetime_from_string(value.replace("_", "-"))
        else:
            result = None
    return result


def feature_key(*id_values):
    """Return key value that defines a unique feature.

    Args:
        *id_values (str): Ordered collection of ID values.

    Returns:
        str
    """
    return clean_whitespace(
        concatenate(*id_values, separator=" | ", nonetype_replacement="")
    )


def feature_key_hash(*id_values):
    """Return key-hash hexadecimal  value that defines a unique feature.

    Args:
        *id_values (str): Ordered collection of ID values.

    Returns:
        str
    """
    return sha256(feature_key(*id_values).encode()).hexdigest()


def force_lowercase(value):
    """Return value converted to lowercase.

    Args:
        value (str): Value to alter.

    Returns:
        str: Altered value.
    """
    if value:
        value = value.lower()
    return value


def force_title_case(value):
    """Return value converted to title case.

    Args:
        value (str): Value to alter.

    Returns:
        str: Altered value.
    """
    if value:
        value = value.title()
    return value


def force_uppercase(value):
    """Return value converted to uppercase.

    Args:
        value (str): Value to alter.

    Returns:
        str: Altered value.
    """
    if value:
        value = value.upper()
    return value


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


def leading_number_sort_key(numbered_string):
    """Return key for sorting strings starting with numbers.

    Args:
        numbered_string (str): String to sort.

    Returns:
        tuple
    """
    if not numbered_string:
        return (-(2 ** 63), "")

    non_numeric_tail = numbered_string.lstrip("0123456789")
    if non_numeric_tail == numbered_string:
        numeric_head = 2 ** 63 - 1
    elif not non_numeric_tail:
        numeric_head = int(numbered_string)
    else:
        numeric_head = int(numbered_string[: -len(non_numeric_tail)])
    return (numeric_head, non_numeric_tail)


def max_value(*values):
    """Return maximum value whle handling empty collections & NoneTypes.
    Args:
        *values: Variable length argument list.

    Returns:
        object
    """
    if not values:
        result = None
    else:
        try:
            result = max(values)
        except TypeError:
            result = max_value(*(value for value in values if value is not None))
    return result


def min_value(*values):
    """Return minimum value whle handling empty collections & NoneTypes.
    Args:
        *values: Variable length argument list.

    Returns:
        object
    """
    if not values:
        result = None
    else:
        try:
            result = min(values)
        except TypeError:
            result = min_value(*(value for value in values if value is not None))
    return result


def parity(*numbers):
    """Return proper parity description for a collection of integers.

    Parity description can be: "Even", "Odd", or "Mixed".

    Args:
        *numbers: Collection of numbers.

    Returns:
        str
    """
    numbers_bitwise = {n & 1 for n in numbers}
    if not numbers_bitwise:
        result = None
    elif len(numbers_bitwise) == 1:
        result = {0: "Even", 1: "Odd"}[numbers_bitwise.pop()]
    else:
        result = "Mixed"
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


def truncate_datetime(value):
    """Return datetime truncated to the day.

    Args:
        value (datetime.datetime): Value to truncate.

    Returns:
        datetime.datetime
    """
    return (
        datetime.datetime(value.year, value.month, value.day)
        if value is not None
        else None
    )
