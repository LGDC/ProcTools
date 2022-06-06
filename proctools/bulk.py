"""Bulk-processing objects."""
from collections import Counter
from dataclasses import asdict
from datetime import date
from functools import partial
from logging import Logger, getLogger
from pathlib import Path
from types import FunctionType
from typing import Any, Dict, Iterable, List, Optional, Union

from arcproc import (
    Dataset,
    Field,
    Procedure,
    SpatialReferenceSourceItem,
    add_field,
    update_field_with_function,
)

from proctools.metadata import Dataset as MetaDataset
from proctools.value import (
    clean_whitespace,
    enforce_yn,
    make_lowercase,
    make_title_case,
    make_uppercase,
)


__all__ = []


LOG: Logger = getLogger(__name__)
"""Module-level logger."""


def add_missing_fields(
    dataset: Union[Path, str, Procedure],
    dataset_metadata: MetaDataset,
    *,
    from_source: bool = False,
) -> List[Field]:
    """Add missing fields listed in dataset metadata object.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        dataset_metadata: Dataset information object to get field information from.
        from_source: Add fields from source dataset if True.

    Returns:
        Field metadata instances for fields.
    """
    add_keys = [
        "name",
        "type",
        "length",
        "precision",
        "scale",
        "is_nullable",
        "is_required",
        "alias",
    ]
    fields = list(
        dataset_metadata.source_fields if from_source else dataset_metadata.out_fields
    )
    for field in fields:
        field = {key: value for key, value in asdict(field).items() if key in add_keys}
        if isinstance(dataset, Procedure):
            dataset.transform(add_field, exist_ok=True, **field)
        else:
            add_field(dataset_path=dataset, exist_ok=True, **field)
    return fields


def bulk_clean_all_whitespace(
    dataset: Union[Path, str, Procedure], *, use_edit_session: bool = False
) -> Counter:
    """Clean whitespace in field values of all text fields in dataset.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        use_edit_session: True if edits are to be made in an edit session.

    Returns:
        Attribute counts for each update-state.
    """
    fields = Dataset(
        dataset.transform_path if isinstance(dataset, Procedure) else dataset
    ).user_fields
    fields = [field for field in fields if field.type.upper() in ["STRING", "TEXT"]]
    states = Counter()
    states.update(
        bulk_clean_whitespace(
            dataset,
            field_names=[field.name for field in fields if field.is_nullable],
            clear_empty_string=True,
            use_edit_session=use_edit_session,
        )
    )
    states.update(
        bulk_clean_whitespace(
            dataset,
            field_names=[field.name for field in fields if not field.is_nullable],
            clear_empty_string=False,
            use_edit_session=use_edit_session,
        )
    )
    return states


def bulk_clean_whitespace(
    dataset: Union[Path, str, Procedure],
    *,
    field_names: Iterable[str],
    clear_empty_string: bool = True,
    use_edit_session: bool = False,
) -> Counter:
    """Clean whitespace in field values.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        field_names: Names of fields to update.
        clear_empty_string: Convert empty string results to None if True.
        use_edit_session: True if edits are to be made in an edit session.

    Returns:
        Attribute counts for each update-state.
    """
    return bulk_update_values_by_function(
        dataset,
        field_names=field_names,
        function=(
            clean_whitespace
            if clear_empty_string
            else partial(clean_whitespace, clear_empty_string=False)
        ),
        use_edit_session=use_edit_session,
    )


def bulk_enforce_yn_values(
    dataset: Union[Path, str, Procedure],
    *,
    field_names: Iterable[str],
    default: Union[str, None] = None,
    use_edit_session: bool = False,
) -> Counter:
    """Enforce usage of only "Y" or "N" in field values.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        field_names: Names of fields to update.
        default: Value to change non-YN values to.
        use_edit_session: True if edits are to be made in an edit session.

    Returns:
        Attribute counts for each update-state.
    """
    return bulk_update_values_by_function(
        dataset,
        field_names=field_names,
        function=partial(enforce_yn, default=default),
        use_edit_session=use_edit_session,
    )


def bulk_make_values_lowercase(
    dataset: Union[Path, str, Procedure],
    *,
    field_names: Iterable[str],
    use_edit_session: bool = False,
) -> Counter:
    """Make field values lowercase.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        field_names: Names of fields to update.
        use_edit_session: True if edits are to be made in an edit session.

    Returns:
        Attribute counts for each update-state.
    """
    return bulk_update_values_by_function(
        dataset,
        field_names=field_names,
        function=make_lowercase,
        use_edit_session=use_edit_session,
    )


def bulk_make_values_title_case(
    dataset: Union[Path, str, Procedure],
    *,
    field_names: Iterable[str],
    part_correction: Optional[Dict[str, str]] = None,
    use_edit_session: bool = False,
) -> Counter:
    """Make field values title case.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        field_names: Names of fields to update.
        part_correction: Mapping of word or other string part to specific output
            correction of base title-casing. Word key must already be in title-cased
            style (i.e. `key == key.title()`).
        use_edit_session: True if edits are to be made in an edit session.

    Returns:
        Attribute counts for each update-state.
    """
    return bulk_update_values_by_function(
        dataset,
        field_names=field_names,
        function=partial(make_title_case, part_correction=part_correction),
        use_edit_session=use_edit_session,
    )


def bulk_make_values_uppercase(
    dataset: Union[Path, str, Procedure],
    *,
    field_names: Iterable[str],
    use_edit_session: bool = False,
) -> Counter:
    """Make field values uppercase.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        field_names: Names of fields to update.
        use_edit_session: True if edits are to be made in an edit session.

    Returns:
        Attribute counts for each update-state.
    """
    return bulk_update_values_by_function(
        dataset,
        field_names=field_names,
        function=make_uppercase,
        use_edit_session=use_edit_session,
    )


def bulk_replace_all_null_values(
    dataset: Union[Path, str, Procedure],
    *,
    date_replacement: date = date.min,
    float_replacement: float = 0.0,
    integer_replacement: int = 0,
    string_replacement: str = "",
    use_edit_session: bool = False,
):
    """Replace NULL values in the all user fields.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        date_replacement: Value to replace NULL-dates with.
        float_replacement: Value to replace NULL-floats with.
        integer_replacement: Value to replace NULL-integers with.
        string_replacement: Value to replace NULL-strings with.
        use_edit_session: True if edits are to be made in an edit session.

    Returns:
        Attribute counts for each update-state.
    """
    type_replacement = {"Date": date_replacement, "String": string_replacement}
    type_replacement["Double"] = type_replacement["Single"] = float_replacement
    type_replacement["Integer"] = type_replacement["SmallInteger"] = integer_replacement
    fields = Dataset(
        dataset.transform_path if isinstance(dataset, Procedure) else dataset
    ).user_fields
    states = Counter()
    for field in fields:
        if field.type not in type_replacement:
            LOG.info(
                "Skipping field `%s`: %s type not covered by function.",
                field.name,
                field.type,
            )
            continue

        states.update(
            bulk_replace_null_values(
                dataset,
                field_names=[field.name],
                replacement_value=type_replacement[field.type],
                use_edit_session=use_edit_session,
            )
        )
    return states


def bulk_replace_null_values(
    dataset: Union[Path, str, Procedure],
    *,
    field_names: Iterable[str],
    replacement_value: Any,
    use_edit_session: bool = False,
) -> Counter:
    """Replace NULL values in field values.

    Notes:
        All fields assumed to have the same data type.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        field_names: Names of fields to update.
        replacement_value: Value to replace NULLs with.
        use_edit_session: True if edits are to be made in an edit session.

    Returns:
        Attribute counts for each update-state.
    """
    return bulk_update_values_by_function(
        dataset,
        field_names=field_names,
        function=(
            lambda x, replacement=replacement_value: replacement if x is None else x
        ),
        use_edit_session=use_edit_session,
    )


def bulk_update_values_by_function(
    dataset: Union[Path, str, Procedure],
    *,
    field_names: Iterable[str],
    function: FunctionType,
    field_as_first_arg: bool = True,
    arg_field_names: Iterable[str] = (),
    kwarg_field_names: Iterable[str] = (),
    spatial_reference_item: SpatialReferenceSourceItem = None,
    use_edit_session: bool = False,
) -> Counter:
    """Update field values by passing them to a function.

    Args:
        dataset: Path to dataset, or ArcProc Procedure instance.
        field_names: Names of fields to update.
        function: Function to get update values.
        field_as_first_arg: True if field value will be the first positional argument.
        arg_field_names: Field names whose values will be the function positional
            arguments (not including primary field).
        kwarg_field_names: Field names whose names & values will be the function keyword
            arguments.
        spatial_reference_item: Item from which the spatial reference for any geometry
            properties will be set to. If set to None, will use spatial reference of
            the dataset.
        use_edit_session: True if edits are to be made in an edit session.

    Returns:
        Attribute counts for each update-state.
    """
    kwargs = {
        "function": function,
        "field_as_first_arg": field_as_first_arg,
        "arg_field_names": arg_field_names,
        "kwarg_field_names": kwarg_field_names,
        "spatial_reference_item": spatial_reference_item,
        "use_edit_session": use_edit_session,
    }
    states = Counter()
    for kwargs["field_name"] in field_names:
        if isinstance(dataset, Procedure):
            states.update(dataset.transform(update_field_with_function, **kwargs))
        else:
            states.update(update_field_with_function(dataset_path=dataset, **kwargs))
    return states
