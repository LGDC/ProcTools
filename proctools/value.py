"""Value-building, -deriving, and -cleaning objects."""
from datetime import date
from datetime import datetime as _datetime
from hashlib import sha256
from logging import Logger, getLogger
from string import punctuation, whitespace
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from unicodedata import combining, normalize

from dateutil.parser import parse
from more_itertools import pairwise


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""

# Adding en-dash & em-dash; string.punctuation only has hyphen.
PUNCTUATION: str = punctuation + "–—"
"""Common punctuation characters."""
TITLE_CASE_EXCEPTIONS: Dict[str, List[str]] = {
    "directional_abbreviations": ["N", "S", "E", "W", "NE", "NW", "SE", "SW"],
    # Keep final punctuation off, already stripped in function.
    "latin_acronyms": ["e.g", "i.e", "etc"],
    "hyphenated_prefixes": ["at-large"],
    "ordinal_numbers": [
        "0th",
        "1st",
        "1th",
        "2nd",
        "2th",
        "3rd",
        "3th",
        "4th",
        "5th",
        "6th",
        "7th",
        "8th",
        "9th",
    ],
    "roman_numerals": [
        "II",
        "III",
        "IV",
        "VI",
        "VII",
        "VIII",
        "IX",
        "XI",
        "XII",
        "XIII",
        "XIV",
        "XV",
        "XVI",
        "XVII",
        "XVIII",
        "XIX",
    ],
    "short_words": [
        # Prepositions.
        "at",
        "by",
        "for",
        "from",
        "in",
        "into",
        "of",
        "on",
        "onto",
        "over",
        "to",
        "with",
        # Conjunctions.
        "and",
        "as",
        "but",
        "for",
        "if",
        "nor",
        "or",
        # Particles.
        "to",
        # Articles.
        "a",
        "an",
        "the",
    ],
}
"""Mapping of tag to collections of exceptions to Python title-casing."""


def any_in_range(*numbers: int, floor: int, ceiling: int) -> bool:
    """Return True if any of the integers are within given range.

    Args:
        *numbers: Integers to evaluate.
        floor: Lowest integer in range.
        ceiling: Highest integer in range.
    """
    return any(number in range(floor, ceiling + 1) for number in numbers)


def clean_whitespace(
    value: Union[str, None], *, clear_empty_string: bool = True
) -> Union[str, None]:
    """Return value with whitespace stripped & deduplicated.

    Args:
        value: Value to alter.
        clear_empty_string: Convert empty string results to None if True.
    """
    if value is not None:
        value = value.strip()
        for character in whitespace:
            while character * 2 in value:
                value = value.replace(character * 2, character)
    if clear_empty_string and not value:
        value = None
    return value


def concatenate(
    *values: Any,
    nonetype_replacement: Optional[str] = None,
    separator: str = " ",
    wrappers: Sequence[str] = ("", ""),
) -> Union[str, None]:
    """Return concatenated string from ordered values with separator.

    Ignores NoneTypes.

    Args:
        *values: Values to concatenate. Values will be converted string if not already.
        nonetype_replacement: Value to replace None-values with. If set to None, will
            skip concatenating.
        separator: String to separate values.
        wrappers: Two string values with which to place before and after each value. For
            example, ("[", "]") will wrap values with square brackets. Default is
            ("", "") - no wrapper.

    Returns:
        Concatenated string if not empty, None otherwise.
    """
    if nonetype_replacement is None:
        values = (str(value) for value in values if value is not None)
    else:
        values = (
            nonetype_replacement if value is None else str(value) for value in values
        )
    concatenated = separator.join(value.join(wrappers) for value in values)
    return concatenated if concatenated else None


def date_as_datetime(value: Union[date, _datetime, None]) -> Union[_datetime, None]:
    """Return date or datetime value zero-time datetime.

    Args:
        value: Date to convert. Allows datetime values for zeroing-out time parts.

    Returns:
        datetime version of date. None if value is None.
    """
    return _datetime(value.year, value.month, value.day) if value else value


def datetime_from_string(value: Union[str, None]) -> Union[_datetime, None]:
    """Extract datetime object from input.

    Args:
        value: Value to extract datetime from.

    Returns:
        Extracted datetime if found. None if datetime not found.
    """
    try:
        result = parse(value) if value else None
    except ValueError:
        if "_" in value:
            result = datetime_from_string(value.replace("_", "-"))
        else:
            result = None
    return result


def feature_key(*id_values: Iterable[Any]) -> Union[str, None]:
    """Return key string that defines a unique feature.

    Args:
        *id_values: Sequence of ID values.
    """
    return clean_whitespace(
        concatenate(
            *id_values,
            nonetype_replacement="",
            separator=" | ",
        ),
    )


def feature_key_hash(*id_values: Iterable[Any]) -> Union[str, None]:
    """Return key-hash hexadecimal string that defines a unique feature.

    Args:
        *id_values: Sequence of ID values.
    """
    key = feature_key(*id_values)
    return sha256(key.encode()).hexdigest() if key is not None else None


def make_lowercase(value: Union[str, None]) -> Union[str, None]:
    """Return value converted to lowercase.

    Args:
        value: Value to convert.
    """
    return value.lower() if value else value


def make_title_case(
    value: Union[str, None], *, part_correction: Optional[Dict[str, str]] = None
) -> Union[str, None]:
    """Return value converted to title case.

    Args:
        value: Value to convert.
        part_correction: Mapping of word or other string part to specific output
            correction of base title-casing. Word key must already be in title-cased
            style (i.e. `key == key.title()`).
    """
    if not value:
        return value

    parts = value.title().split()
    new_value = ""
    for i, part in enumerate(parts):
        # Need to strip for custom corrections to work around punctuation.
        stripped = {"start": "", "end": ""}
        while any(part.startswith(character) for character in PUNCTUATION):
            stripped["start"] += part[0]
            part = part[1:]
        while any(part.endswith(character) for character in PUNCTUATION):
            stripped["end"] = part[-1] + stripped["end"]
            part = part[:-1]
        # Python capitalizes letters right after an apostrophe. Correct into end-strip.
        if part.endswith("'S") or part.endswith("’S"):
            stripped["end"] = "'s" + stripped["end"]
            part = part[:-2]
        # Keep certain short words lowercase.
        if part.lower() in TITLE_CASE_EXCEPTIONS["short_words"]:
            # Skip for first & last word.
            if i not in [0, len(parts) - 1]:
                part = part.lower()
        elif part.lower() in TITLE_CASE_EXCEPTIONS["latin_acronyms"]:
            # Skip for first word.
            if i != 0:
                part = part.lower()
        # Keep certain short words uppercase.
        elif part.upper() in TITLE_CASE_EXCEPTIONS["directional_abbreviations"]:
            part = part.upper()
        elif part.upper() in TITLE_CASE_EXCEPTIONS["roman_numerals"]:
            part = part.upper()
        # Certain hyphenated compounds lowercase the second word.
        elif part.lower() in TITLE_CASE_EXCEPTIONS["hyphenated_prefixes"]:
            part = part[0].upper() + part[1:].lower()
        # Capitalize letter after "Mc" (names).
        elif part != "Mc" and part.startswith("Mc"):
            part = part[:2] + part[2].upper() + part[3:]
        # Python capitalizes letters right after a number.
        if part[-3:].lower() in TITLE_CASE_EXCEPTIONS["ordinal_numbers"]:
            part = part[:-3] + part[-3:].lower()
        if part_correction and part in part_correction:
            part = part_correction[part]
        # Weird edge-case: the initial "A.".
        if part == "a" and stripped["end"].startswith("."):
            part = part.upper()
        # Reattach stripped characters.
        part = stripped["start"] + part + stripped["end"]
        new_value += part if i == 0 else " " + part
    return new_value


def make_uppercase(value: Union[str, None]) -> Union[str, None]:
    """Return value converted to uppercase.

    Args:
        value: Value to convert.
    """
    return value.upper() if value else value


def enforce_yn(
    value: Union[str, None], default: Union[str, None] = None
) -> Union[str, None]:
    """Return given value if valid "Y" or "N" representation; otherwise return default.

    Args:
        value: Value to convert.
        default: String (or None) to force if value not valid.

    Returns:
        str: Altered value.
    """
    return value if value in ("n", "N", "y", "Y") else default


def is_numeric(value: Union[str, None], *, nonetype_ok: bool = True) -> bool:
    """Return True if string value is numeric.

    Props to: http://pythoncentral.io/
        how-to-check-if-a-string-is-a-number-in-python-including-unicode/

    Args:
        value: Value to evaluate.
        nonetype_ok: If True, return True for NoneType values.
    """
    if value is None:
        return nonetype_ok

    try:
        float(value)
    except ValueError:
        result = False
    else:
        result = str(value).lower() not in ("nan", "inf")
    return result


def leading_number_sort_key(value: Union[str, None]) -> Tuple[int, str]:
    """Return key for sorting string that might start with numbers.

    Args:
        value: Value to evaluate.
    """
    if not value:
        return (-(2**63), "")

    tail = value.lstrip("0123456789")
    # No numeric head - set numeric sort value to one higher than None/empty:
    if tail == value:
        numeric_head = 2**63 - 1
    elif not tail:
        numeric_head = int(value)
    else:
        numeric_head = int(value[: -len(tail)])
    return (numeric_head, tail)


def make_zero_filled(value: Union[str, None], width: int) -> Union[str, None]:
    """Return value with zero-filling.

    Args:
        value: Value to convert.
        width: Width of zero-filling.
    """
    return value.zfill(width) if value is not None else None


def max_value(*values: Any) -> Any:
    """Return maximum value, handling empty collections & NoneTypes.

    Values must be types comparable among all types given.

    Args:
        *values: Values to compare.
    """
    values = [value for value in values if value is not None]
    return max(values) if values else None


def min_value(*values: Any) -> Any:
    """Return minimum value, handling empty collections & NoneTypes.

    Values must be types comparable among all types given.

    Args:
        *values: Values to compare.
    """
    values = [value for value in values if value is not None]
    return min(*values) if values else None


def parity(*values: int) -> str:
    """Return proper parity description for a collection of integers.

    Args:
        *values: Values to compare.

    Returns:
        Parity description: "Even", "Odd", or "Mixed".
    """
    if not values:
        return None

    bitwise_values = tuple(set(n & 1 for n in values))
    if len(bitwise_values) == 1:
        result = "Even" if bitwise_values[0] == 0 else "Odd"
    else:
        result = "Mixed"
    return result


def remove_diacritics(value: Union[str, None]) -> Union[str, None]:
    """Return value converted with diacritics removed.

    Args:
        value: Value to convert.
    """
    if not value:
        return value

    return "".join(char for char in normalize("NFKD", value) if not combining(char))


def same_string_casefold(*values: Union[str, None]) -> bool:
    """Return True if strings are same, normalized & ignoring case.

    Args:
        *values: Values to compare.
    """
    if len(values) <= 1:
        same = True
    elif any(val is None for val in values):
        same = all(val is None for val in values)
    else:
        same = all(
            normalize("NFKD", value.casefold())
            == normalize("NFKD", cmp_value.casefold()).casefold()
            for value, cmp_value in pairwise(values)
        )
    return same


def slugify(text: str, *, separator: str = "-", force_lowercase: bool = True) -> str:
    """Return text in slug-form.

    Args:
        text: String to slugify.
        separator: Separator to replace punctuation & whitespace.
        force_lowercase: Make all letters lowercase if True; keep same if False.
    """
    slug = text.lower() if force_lowercase else text
    for char in punctuation + whitespace:
        slug = slug.replace(char, separator)
    while separator * 2 in slug:
        slug = slug.replace(separator * 2, separator)
    if slug.startswith(separator):
        slug = slug[len(separator) :]
    if slug.endswith(separator):
        slug = slug[: -len(separator)]
    return slug


def truncate_datetime(value: Union[_datetime, None]) -> Union[_datetime, None]:
    """Return datetime truncated to the day.

    Args:
        value: Value to truncate.
    """
    return _datetime(value.year, value.month, value.day) if value else value
