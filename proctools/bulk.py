"""Bulk-processing objects."""
from dataclasses import asdict
import datetime
from functools import partial
import logging
from pathlib import Path
from typing import Any, Iterable, Optional, Union

import arcproc
from arcproc.metadata import Dataset as _Dataset

from .meta import Dataset2, Field  # pylint: disable=relative-beyond-top-level
from . import value  # pylint: disable=relative-beyond-top-level


__all__ = []


LOG: logging.Logger = logging.getLogger(__name__)
"""Module-level logger."""


def add_missing_fields(dataset, dataset_metadata, tags=None, from_source=False):
    """Add missing fields listed in dataset meta object.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        dataset_metadata (proctools.meta.Dataset, proctools.meta.Dataset2)
        tags (iter, None): Collection of tags a field must have one of in metadata for
            field to be added. If tags is None, all fields listed in metadata are added.
        from_source (bool): Add fields from source dataset if True.
    """
    if isinstance(dataset_metadata, Dataset2):
        fields = (
            dataset_metadata.source_fields
            if from_source
            else dataset_metadata.out_fields
        )
    elif tags:
        fields = [
            field
            for field in dataset_metadata.fields
            if set(field.get("tags")) & set(tags)
        ]
    else:
        fields = dataset_metadata.fields
    if isinstance(dataset, arcproc.managers.Procedure):
        proc = dataset
        for field in fields:
            if isinstance(field, Field):
                field = asdict(field)
            proc.transform(arcproc.dataset.add_field, exist_ok=True, **field)
    else:
        dataset_path = dataset
        for field in fields:
            field = asdict(field)
            arcproc.dataset.add_field(dataset_path, exist_ok=True, **field)


def clean_all_whitespace(dataset, **kwargs):
    """Clean whitespace in values of all text fields dataset.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)
    if isinstance(dataset, arcproc.managers.Procedure):
        fields = _Dataset(dataset.transform_path).user_fields
    else:
        fields = _Dataset(dataset).user_fields
    for _field in fields:
        if _field.type.upper() in ("STRING", "TEXT"):
            update_by_function(
                dataset,
                field_names=[_field.name],
                function=(
                    value.clean_whitespace
                    if _field.is_nullable
                    else value.clean_whitespace_without_clear
                ),
                use_edit_session=kwargs["use_edit_session"],
            )


def clean_whitespace(dataset, field_names, **kwargs):
    """Clean whitespace in values of fields.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clean.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)
    update_by_function(
        dataset,
        field_names,
        function=value.clean_whitespace,
        use_edit_session=kwargs["use_edit_session"],
    )


def clean_whitespace_without_clear(dataset, field_names, **kwargs):
    """Clean whitespace on values of fields without converting empty values to None.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clean.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)
    update_by_function(
        dataset,
        field_names,
        function=value.clean_whitespace_without_clear,
        use_edit_session=kwargs["use_edit_session"],
    )


def clear_all_values(dataset, field_names, **kwargs):
    """Clear all values, changing them to NoneTypes.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)
    if isinstance(dataset, arcproc.managers.Procedure):
        proc = dataset
        for field_name in field_names:
            proc.transform(
                arcproc.attributes.update_by_value,
                field_name=field_name,
                value=None,
                use_edit_session=kwargs["use_edit_session"],
            )
    else:
        dataset_path = dataset
        for field_name in field_names:
            arcproc.attributes.update_by_value(
                dataset_path,
                field_name,
                value=None,
                use_edit_session=kwargs["use_edit_session"],
            )


def clear_non_numeric_text(dataset, field_names, **kwargs):
    """Clear non-numeric text values of fields.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)
    update_by_function(
        dataset,
        field_names,
        function=lambda x: x if value.is_numeric(x) else None,
        use_edit_session=kwargs["use_edit_session"],
    )


def clear_nonpositive(dataset, field_names, **kwargs):
    """Clear nonpositive values of fields.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)

    def value_function(val):
        try:
            result = val if float(val) > 0 else None
        except (ValueError, TypeError):
            result = None
        return result

    update_by_function(
        dataset,
        field_names,
        function=value_function,
        use_edit_session=kwargs["use_edit_session"],
    )


def force_lowercase(dataset, field_names, **kwargs):
    """Force lowercase in values of fields.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)
    update_by_function(
        dataset,
        field_names,
        function=value.force_lowercase,
        use_edit_session=kwargs["use_edit_session"],
    )


def force_title_case(dataset, field_names, correction_map=None, **kwargs):
    """Force title case in values of fields.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clear.
        correction_map (dict, None): Mapping of word or other string part with specific
            output correction to title-casing. Word key must already be in title-cased
            style (i.e. key = `key.title()`).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)
    update_by_function(
        dataset,
        field_names,
        function=partial(value.force_title_case, correction_map=correction_map),
        use_edit_session=kwargs["use_edit_session"],
    )


def force_uppercase(dataset, field_names, **kwargs):
    """Force uppercase in values of fields.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)
    update_by_function(
        dataset,
        field_names,
        function=value.force_uppercase,
        use_edit_session=kwargs["use_edit_session"],
    )


def force_yn(dataset, field_names, default=None, **kwargs):
    """Ensure only "Y" or "N" in values of fields.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clear.
        default (str, None): Value to change non-YN values to.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("use_edit_session", False)
    update_by_function(
        dataset,
        field_names,
        function=partial(value.force_yn, default=default),
        use_edit_session=kwargs["use_edit_session"],
    )


def insert_features_from_paths(
    dataset, insert_dataset_paths, field_names=None, **kwargs
):
    """Insert features into the dataset from given dataset path.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        insert_dataset_paths (iter): Collection of paths with features to insert.
        field_names (iter): Collection of field names to insert. Listed field must be
            present in both datasets. If field_names is None, all fields will be
            inserted.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        insert_where_sql (str): SQL where-clause for insert-dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
    """
    kwargs.setdefault("insert_where_sql")
    kwargs.setdefault("use_edit_session", False)
    if isinstance(dataset, arcproc.managers.Procedure):
        proc = dataset
        for insert_dataset_path in insert_dataset_paths:
            proc.transform(
                arcproc.features.insert_from_path,
                insert_dataset_path=insert_dataset_path,
                field_names=field_names,
                insert_where_sql=kwargs["insert_where_sql"],
                use_edit_session=kwargs["use_edit_session"],
            )
    else:
        dataset_path = dataset
        for insert_dataset_path in insert_dataset_paths:
            arcproc.features.insert_from_path(
                dataset_path,
                insert_dataset_path,
                field_names,
                insert_where_sql=kwargs["insert_where_sql"],
                use_edit_session=kwargs["use_edit_session"],
            )


def rename_fields(dataset, field_name_change_map):
    """Rename fields using name change map.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_name_change_map (dict): Mapping of old to new field name.
    """
    if isinstance(dataset, arcproc.managers.Procedure):
        proc = dataset
        for field_name, new_field_name in field_name_change_map.items():
            proc.transform(
                arcproc.dataset.rename_field,
                field_name=field_name,
                new_field_name=new_field_name,
            )
    else:
        dataset_path = dataset
        for field_name, new_field_name in field_name_change_map.items():
            arcproc.dataset.rename_field(dataset_path, field_name, new_field_name)


def replace_all_null_values(
    dataset: Union[Path, str, arcproc.managers.Procedure],
    date_replacement: datetime.date = datetime.date.min,
    integer_replacement: int = 0,
    float_replacement: float = 0.0,
    string_replacement: str = "",
    *,
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
):
    """Replace NULL values in the all user fields.

    Args:
        dataset: Path to the dataset, or Procedure instance with transform dataset.
        date_replacement: Value to replace NULL-dates with.
        integer_replacement: Value to replace NULL-integers with.
        float_replacement: Value to replace NULL-floats with.
        string_replacement: Value to replace NULL-strings with.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: Updates are done in an edit session if True.
    """
    type_field_names = {}
    type_replacement = {"Date": date_replacement, "String": string_replacement}
    type_replacement["Double"] = type_replacement["Single"] = float_replacement
    type_replacement["Integer"] = type_replacement["SmallInteger"] = integer_replacement
    if isinstance(dataset, arcproc.managers.Procedure):
        fields = _Dataset(dataset.transform_path).user_fields
    else:
        fields = _Dataset(dataset).user_fields
    for field in fields:
        if field["type"] not in type_field_names:
            type_field_names[field["type"]] = []
        type_field_names[field["type"]].append(field["name"])
    for _type, field_names in sorted(type_field_names.items()):
        if _type not in type_replacement:
            LOG.info(
                "Skipping fields `%s`: %s type not covered by function.",
                field_names,
                _type,
            )
            continue

        replace_null_values(
            dataset,
            field_names,
            replacement_value=type_replacement[_type],
            dataset_where_sql=dataset_where_sql,
            use_edit_session=use_edit_session,
        )


def replace_null_values(
    dataset: Union[Path, str, arcproc.managers.Procedure],
    field_names: Iterable[str],
    replacement_value: Any,
    *,
    dataset_where_sql: Optional[str] = None,
    use_edit_session: bool = False,
):
    """Replace NULL values in the given fields.

    Notes:
        All fields assumed to have the same data type

    Args:
        dataset: Path to the dataset, or Procedure instance with transform dataset.
        field_names: Names of the fields.
        replacement_value: Value to replace NULLs with.
        dataset_where_sql: SQL where-clause for dataset subselection.
        use_edit_session: Updates are done in an edit session if True.
    """

    def replace_null(_value, replacement_value):
        return replacement_value if _value is None else _value

    update_by_function(
        dataset,
        field_names,
        function=partial(replace_null, replacement_value=replacement_value),
        dataset_where_sql=dataset_where_sql,
        use_edit_session=use_edit_session,
    )


def update_by_function(dataset, field_names, function, **kwargs):
    """Update given fields by provided function.

    Args:
        dataset (pathlib.Path, str, arcproc.managers.Procedure): Path to dataset, or
            Procedure instance.
        field_names (iter): Collection of field names to clear.
        function (types.FunctionType): Function to get values from.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        field_as_first_arg (bool): True if field value will be the first positional
            argument. Default is True.
        arg_field_names (iter): Field names whose values will be the positional
            arguments (not including primary field).
        kwarg_field_names (iter): Field names whose names & values will be the method
            keyword arguments.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the spatial reference for the output
            geometry property will be derived. If not specified or None, the spatial
            reference of the dataset is used as the default.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.
    """
    kwargs.setdefault("field_as_first_arg", True)
    kwargs.setdefault("arg_field_names", [])
    kwargs.setdefault("kwarg_field_names", [])
    kwargs.setdefault("dataset_where_sql")
    kwargs.setdefault("spatial_reference_item")
    kwargs.setdefault("use_edit_session", False)
    if isinstance(dataset, arcproc.managers.Procedure):
        proc = dataset
        for field_name in field_names:
            proc.transform(
                arcproc.attributes.update_by_function,
                field_name=field_name,
                function=function,
                field_as_first_arg=kwargs["field_as_first_arg"],
                arg_field_names=kwargs["arg_field_names"],
                kwarg_field_names=kwargs["kwarg_field_names"],
                dataset_where_sql=kwargs["dataset_where_sql"],
                spatial_reference_item=kwargs["spatial_reference_item"],
                use_edit_session=kwargs["use_edit_session"],
            )
    else:
        dataset_path = dataset
        for field_name in field_names:
            arcproc.attributes.update_by_function(
                dataset_path,
                field_name,
                function,
                field_as_first_arg=kwargs["field_as_first_arg"],
                arg_field_names=kwargs["arg_field_names"],
                kwarg_field_names=kwargs["kwarg_field_names"],
                dataset_where_sql=kwargs["dataset_where_sql"],
                spatial_reference_item=kwargs["spatial_reference_item"],
                use_edit_session=kwargs["use_edit_session"],
            )
