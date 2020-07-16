"""Bulk-processing objects."""
from functools import partial
import os

# import arcproc  # Imported locally to avoid slow imports.

from . import value


__all__ = []


def add_missing_fields(dataset, dataset_metadata, tags=None):
    """Add missing fields listed in dataset meta object.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        dataset_metadata (proctools.meta.Dataset)
        tags (iter, None): Collection of tags a field must have one of in metadata for
            field to be added. If tags is None, all fields listed in metadata are added.
    """
    import arcproc

    tags = set(tags) if tags else set()
    if not tags:
        fields_meta = dataset_metadata.fields
    else:
        fields_meta = (
            field for field in dataset_metadata.fields if set(field.get("tags")) & tags
        )
    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(dataset.transform, transformation=arcproc.dataset.add_field)
    else:
        func = partial(arcproc.dataset.add_field, dataset_path=dataset)
    for meta in fields_meta:
        func(exist_ok=True, **meta)


def clean_whitespace(dataset, field_names, **kwargs):
    """Clean whitespace in values of fields.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_names (iter): Collection of field names to clean.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.attributes.update_by_function`.
    """
    import arcproc

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_function
        )
    else:
        func = partial(arcproc.attributes.update_by_function, dataset_path=dataset)
    for name in field_names:
        func(field_name=name, function=value.clean_whitespace, **kwargs)


def clean_whitespace_without_clear(dataset, field_names, **kwargs):
    """Clean whitespace on values of fields without converting empty values to None.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_names (iter): Collection of field names to clean.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.attributes.update_by_function`.
    """
    import arcproc

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_function
        )
    else:
        func = partial(arcproc.attributes.update_by_function, dataset_path=dataset)
    for name in field_names:
        func(field_name=name, function=value.clean_whitespace_without_clear, **kwargs)


def clear_all_values(dataset, field_names, **kwargs):
    """Clear all values, changing them to NoneTypes.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.attributes.update_by_value`.
    """
    import arcproc

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_value
        )
    else:
        func = partial(arcproc.attributes.update_by_value, dataset_path=dataset)
    for name in field_names:
        func(field_name=name, value=None, **kwargs)


def clear_non_numeric_text(dataset, field_names, **kwargs):
    """Clear non-numeric text values of fields.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.attributes.update_by_function`.
    """
    import arcproc

    def val_func(val):
        return val if value.is_numeric(val) else None

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_function
        )
    else:
        func = partial(arcproc.attributes.update_by_function, dataset_path=dataset)
    for name in field_names:
        func(field_name=name, function=val_func, **kwargs)


def clear_nonpositive(dataset, field_names, **kwargs):
    """Clear nonpositive values of fields.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.attributes.update_by_function`.
    """
    import arcproc

    def val_func(val):
        try:
            result = val if float(val) > 0 else None
        except (ValueError, TypeError):
            result = None
        return result

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_function
        )
    else:
        func = partial(arcproc.attributes.update_by_function, dataset_path=dataset)
    for name in field_names:
        func(field_name=name, function=val_func, **kwargs)


def etl_dataset(output_path, source_path=None, **kwargs):
    """Run basic ETL for dataset.

    Args:
        output_path (str): Path of the dataset to load/update.
        source_path (str): Path of the dataset to extract. If None, will initialize
            transform dataset with the output path schema.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        etl_name (str): Name to give the ETL operation.
        extract_where_sql (str): SQL where-clause for extract subselection.
        field_name_change_map (dict): Mapping of field names to their replacement name.
        insert_dataset_paths (iter): Collection of dataset paths to insert features
            from.
        field_value_transfer_map (dict): Mapping of source to destination field name.
        clean_whitespace_field_names (iter): Collection of field names to clean their
            values of excess whitespace.
        dissolve_field_names (iter): Collection of field names to dissolve features on.
        new_unique_ids_field_name (iter): Field name to assign unique IDs to.
        adjust_for_shapefile (bool): Flag to indicate running
            `arcproc.combo.adjust_for_shapefile` on dataset before loading.
        xy_tolerance (float, str): Representation of a distance for operations that can
            interpret a tolerance.
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.

    Returns:
        collections.Counter: Counts for each update type.
    """
    import arcproc

    with arcproc.managers.Procedure(
        kwargs.get("etl_name", os.path.basename(output_path))
    ) as proc:
        # Init.
        if source_path:
            proc.extract(source_path, extract_where_sql=kwargs.get("extract_where_sql"))
        else:
            proc.init_schema(output_path)
        rename_fields(proc, kwargs.get("field_name_change_map", {}))
        # Insert features.
        insert_features_from_paths(proc, kwargs.get("insert_dataset_paths", []))
        # Alter attributes.
        clean_whitespace(proc, kwargs.get("clean_whitespace_field_names", []))
        transfer_field_values(proc, kwargs.get("field_value_transfer_map", {}))
        # Combine features.
        if kwargs.get("dissolve_field_names"):
            proc.transform(
                arcproc.features.dissolve,
                dissolve_field_names=kwargs["dissolve_field_names"],
                tolerance=kwargs.get("xy_tolerance"),
            )
        # Finalize attributes.
        if kwargs.get("new_unique_ids_field_name"):
            proc.transform(
                arcproc.attributes.update_by_unique_id,
                field_name=kwargs["new_unique_ids_field_name"],
            )
        if kwargs.get("adjust_for_shapefile"):
            proc.transform(arcproc.combo.adjust_for_shapefile)
        feature_count = proc.load(
            output_path, use_edit_session=kwargs.get("use_edit_session", False)
        )
        # Loading shapefiles destroys spatial indexes: restore after load.
        if kwargs.get("adjust_for_shapefile"):
            arcproc.dataset.add_index(
                output_path, field_names=["shape"], fail_on_lock_ok=True
            )
    return feature_count


def force_lowercase(dataset, field_names, **kwargs):
    """Force lowercase in values of fields.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.attributes.update_by_function`.
    """
    import arcproc

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_function
        )
    else:
        func = partial(arcproc.attributes.update_by_function, dataset_path=dataset)
    for name in field_names:
        func(field_name=name, function=value.force_lowercase, **kwargs)


def force_title_case(dataset, field_names, **kwargs):
    """Force title case in values of fields.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.attributes.update_by_function`.
    """
    import arcproc

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_function
        )
    else:
        func = partial(arcproc.attributes.update_by_function, dataset_path=dataset)
    for name in field_names:
        func(field_name=name, function=value.force_title_case, **kwargs)


def force_uppercase(dataset, field_names, **kwargs):
    """Force uppercase in values of fields.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_names (iter): Collection of field names to clear.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.attributes.update_by_function`.
    """
    import arcproc

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_function
        )
    else:
        func = partial(arcproc.attributes.update_by_function, dataset_path=dataset)
    for name in field_names:
        func(field_name=name, function=value.force_uppercase, **kwargs)


def force_yn(dataset, field_names, default=None, **kwargs):
    """Ensure only "Y" or "N" in values of fields.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_names (iter): Collection of field names to clear.
        default (str, None): Value to change non-YN values to.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.attributes.update_by_function`.
    """
    import arcproc

    val_func = partial(value.force_yn, default=default)
    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_function
        )
    else:
        func = partial(arcproc.attributes.update_by_function, dataset_path=dataset)
    for name in field_names:
        func(field_name=name, function=val_func, **kwargs)


def insert_features_from_paths(dataset, insert_dataset_paths, **kwargs):
    """Insert features into the dataset from given dataset path.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        insert_dataset_paths (iter): Collection of paths with features to insert.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        See keyword arguments for `arcproc.features.insert_from_path`.
    """
    import arcproc

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.features.insert_from_path
        )
    else:
        func = partial(arcproc.features.insert_from_path, dataset_path=dataset)
    for path in insert_dataset_paths:
        func(insert_dataset_path=path, **kwargs)


def rename_fields(dataset, field_name_change_map):
    """Rename fields using name change map.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_name_change_map (dict): Mapping of old to new field name.
    """
    import arcproc

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(dataset.transform, transformation=arcproc.dataset.rename_field)
    else:
        func = partial(arcproc.dataset.rename_field, dataset_path=dataset)
    for old_name, new_name in field_name_change_map.items():
        func(field_name=old_name, new_field_name=new_name)


def transfer_field_values(dataset, field_value_transfer_map):
    """Transfer field valuess using field name transfer map.

    Args:
        dataset (str, arcproc.managers.Procedure): Path to dataset, or Procedure
            instance.
        field_value_transfer_map (dict): Mapping of source to destination field name.
    """
    import arcproc

    if isinstance(dataset, arcproc.managers.Procedure):
        func = partial(
            dataset.transform, transformation=arcproc.attributes.update_by_field
        )
    else:
        func = partial(arcproc.attributes.update_by_field, dataset_path=dataset)
    for source_name, destination_name in field_value_transfer_map.items():
        func(field_name=destination_name, source_field_name=source_name)
