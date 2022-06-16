"""ArcGIS Online & other portal objects."""
from collections import Counter
from datetime import datetime as _datetime
from logging import DEBUG, Logger, getLogger
from pathlib import Path
from tempfile import gettempdir
from time import sleep
from typing import Any, Iterable, Mapping, Optional, Union

from arcgis.features import Feature, FeatureLayer, Table
from arcgis.gis import GIS, Item, User
from arcproc import (
    Workspace,
    compress_dataset,
    copy_dataset,
    create_file_geodatabase,
    delete_dataset,
)

from proctools.filesystem import archive_folder
from proctools.misc import log_entity_states


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""


def delete_layer_features(
    layer: Union[FeatureLayer, Table],
    *,
    delete_where_sql: str = "1 = 1",
    rollback_on_failure: bool = True,
) -> int:
    """Delete features in a layer or table.

    Args:
        layer: Feature layer or table to delete feature from.
        delete_where_sql: SQL where-clause to choose features to delete. Default (1 = 1)
            will delete all features.
        rollback_on_failure: Deletes should only be applied if all deletes succeed if
            True.

    Returns:
        Number of features deleted.
    """
    if not delete_where_sql:
        delete_where_sql = "1 = 1"
    before_delete_count = layer.query(return_count_only=True)
    try:
        result = layer.delete_features(
            where=delete_where_sql,
            return_delete_results=False,
            rollback_on_failure=rollback_on_failure,
        )
    # The API uses a broad exception here - lame. Check message to limit catch.
    except Exception as error:  # pylint: disable=broad-except
        if str(error) == "Your request has timed out.\n(Error Code: 504)":
            LOG.info("Delete request timed out.")
            check_tries, wait_seconds = 60, 60
            for i in range(1, check_tries + 1):
                sleep(wait_seconds)
                LOG.info("Checking completion: Try %s.", i)
                if layer.query(return_count_only=True) == 0:
                    result = {"success": True}
                    LOG.info("Delete request completed.")
                    break

            else:
                raise RuntimeError("Cannot verify delete request completed.") from error

        else:
            raise

    if not result["success"]:
        LOG.warning(result)
    return before_delete_count - layer.query(return_count_only=True)


def get_item(
    site: GIS, item_name: str, exclude_item_types: Optional[Iterable[str]] = None
) -> Item:
    """Return Item from ArcGIS site.

    Requires item name to be unique.

    Args:
        site: ArcGIS site to search.
        item_name: Name of layer to return.
        exclude_item_types: Listing of item types to exclude.
    """
    if exclude_item_types is None:
        exclude_item_types = set()
    else:
        exclude_item_types = set(exclude_item_types)
    query = f"""title:"{item_name}\""""
    items = [
        item
        for item in site.content.search(query, max_items=100)
        if item.title == item_name and item.type not in exclude_item_types
    ]
    if len(items) == 1:
        item = items[0]
    else:
        raise ValueError(
            f"Item `{item_name}` "
            + ("does not exist" if len(items) == 0 else "name not unique")
            + f" on {site.url}"
        )
    return item


def get_layer(
    site: GIS, layer_name: str, collection_name: Optional[str] = None
) -> Union[FeatureLayer, Table]:
    """Return FeatureLayer or Table from ArcGIS site.

    Requires collection name to be unique. Also requires layer/table name to be unique
        within the collection.

    Args:
        site: ArcGIS site to search.
        layer_name: Name of layer to return.
        collection_name: Name of feature layer collection layer belongs to. If not
            provided, will assume collection shares same name as layer.
    """
    if not collection_name:
        collection_name = layer_name
    query = f"""title:"{collection_name}\""""
    collections = [
        collection
        for collection in site.content.search(
            query, item_type="Feature Layer Collection", max_items=100
        )
        if collection.title == collection_name
    ]
    if len(collections) == 1:
        collection = collections[0]
    else:
        raise ValueError(
            f"Feature layer collection `{collection_name}` "
            + ("does not exist" if len(collections) == 0 else "name not unique")
            + f" on {site.url}"
        )
    layers = [
        layer
        for attr in ["layers", "tables"]
        for layer in getattr(collection, attr)
        if layer.properties["name"] == layer_name
    ]
    if len(layers) == 1:
        layer = layers[0]
    else:
        raise ValueError(
            f"Layer `{collection_name}` "
            + ("does not exist" if len(layers) == 0 else "name not unique")
            + f" in `{collection_name} collection"
        )
    return layer


def get_user(site: GIS, username: str) -> User:
    """Return User from ArcGIS site.

    Requires username to be unique.

    Args:
        site: ArcGIS site to search.
        username: Name of user to return.
    """
    query = f"""username:"{username}\""""
    users = [user for user in site.users.search(query) if user.username == username]
    if len(users) == 1:
        user = users[0]
    else:
        raise ValueError(
            f"User `{username}` "
            + ("does not exist" if len(users) == 0 else "name not unique")
            + f" on {site.url}"
        )
    return user


def load_feature_layer(configuration: Mapping[str, Any], site: GIS) -> Counter:
    """Load the ArcGIS site feature layer corresponding to the source dataset.

    Args:
        configuration: Configuration details for feature layer.
        site: API manager object for portal.

    Returns:
        Counts for each update action.
    """
    layer_name = configuration["layer_name"]
    LOG.info("Start: Load feature layer `%s` on %s.", layer_name, site.url)
    update_count = Counter()
    if "update_exception" in configuration:
        LOG.info(
            "Layer `%s` not updated. %s", layer_name, configuration["update_exception"]
        )
        return update_count

    collection_name = configuration.get("collection_name", layer_name)
    layer = get_layer(site, layer_name, collection_name)
    LOG.info("Uploading source dataset...")
    geodatabase = upload_dataset_as_geodatabase(
        site,
        dataset_path=configuration["dataset"].path,
        geodatabase_name=f"{layer_name}__Temp_{_datetime.now():%Y_%m_%d_T%H%M}.gdb",
        tags=["Temporary"],
    )
    LOG.info("Deleting features...")
    update_count["deleted"] = layer.query(return_count_only=True)
    result = layer.manager.truncate()
    if not result["success"]:
        raise RuntimeError("Delete failed.")

    LOG.info("Inserting features...")
    result = layer.append(
        item_id=geodatabase.id,
        upload_format="filegdb",
        source_table_name=layer_name,
        return_messages=True,
    )
    if not result[0]:
        raise RuntimeError(f"Insert failed - {result[1]}")

    update_count["inserted"] = layer.query(return_count_only=True)
    geodatabase.delete()
    log_entity_states("layer features", update_count, logger=LOG)
    LOG.info("End: Load.")
    return update_count


def update_feature_attribute(feature: Feature, field_name: str, *, value: Any) -> bool:
    """Update field attribute value on feature if necessary & return True if updated.

    Attributes:
        feature: Feature object to potentially update.
        field_name: Name of field to update attribute value.
        value: Value to use for update.
    """
    if feature.get_value(field_name) != value:
        # set_value cannot change a populated value to None/NULL. Use as_dict setter.
        # return feature.set_value(field_name, value)
        feature.as_dict["attributes"][field_name] = value
        return True

    return False


def upload_dataset_as_geodatabase(
    site: GIS,
    dataset_path: Path,
    geodatabase_name: str,
    folder_name: Optional[str] = None,
    tags: Optional[list] = None,
) -> Item:
    """Upload dataset as a file geodatabase to ArcGIS site.

    Args:
        site: API manager object for portal.
        dataset_path: Path to dataset.
        geodatabase_name: Name for output geodatabase.
        folder_name: Name of folder on site to place geodatabase into. If None, the
            geodatabase will be placed in the root folder.
        tags: List of tags to assign to the geodatabase on the site.

    Returns:
        Uploaded file geodatabase item.
    """
    geodatabase_path = Path(gettempdir(), geodatabase_name)
    create_file_geodatabase(geodatabase_path, log_level=DEBUG)
    # Remove enterprise DB schema.
    if Workspace(dataset_path.parent).is_enterprise_database:
        dataset_name = dataset_path.name.split(".", maxsplit=1)[-1]
    else:
        dataset_name = dataset_path.name
    output_path = geodatabase_path / dataset_name
    copy_dataset(dataset_path, output_path=output_path, log_level=DEBUG)
    compress_dataset(output_path, log_level=DEBUG)
    zip_filepath = geodatabase_path.with_suffix(".zip")
    archive_folder(
        folder_path=geodatabase_path,
        archive_path=zip_filepath,
        exclude_patterns=[".lock", ".zip"],
        include_base_folder=True,
    )
    delete_dataset(geodatabase_path, log_level=DEBUG)
    # pylint: disable=no-member
    geodatabase = site.content.add(
        item_properties={"type": "File Geodatabase", "tags": tags},
        # arcgis1.9.1: Convert to str.
        data=str(zip_filepath),
        folder=folder_name,
    )
    # pylint: enable=no-member
    zip_filepath.unlink()
    return geodatabase
