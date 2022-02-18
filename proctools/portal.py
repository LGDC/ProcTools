"""ArcGIS Online & other portal objects."""
import logging
from pathlib import Path
from tempfile import gettempdir
from typing import Optional, Union

from arcgis.gis import GIS, Item
from arcgis.features import FeatureLayer, Table

import arcproc

from proctools.filesystem import archive_folder


__all__ = []

LOG: logging.Logger = logging.getLogger(__name__)
"""Module-level logger."""


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
            query, item_type="Feature Layer Collection"
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
    LOG.info("Start: Upload `%s` as geodatabase to %s.", dataset_path, site.url)
    geodatabase_path = Path(gettempdir(), geodatabase_name)
    arcproc.workspace.create_file_geodatabase(geodatabase_path, log_level=logging.DEBUG)
    dataset_name = dataset_path.name
    if dataset_name.lower().startswith("dbo."):
        dataset_name = dataset_name[4:]
    output_path = geodatabase_path / dataset_name
    arcproc.dataset.copy(dataset_path, output_path, log_level=logging.DEBUG)
    arcproc.dataset.compress(output_path, log_level=logging.DEBUG)
    zip_filepath = geodatabase_path.with_suffix(".zip")
    archive_folder(
        folder_path=geodatabase_path,
        archive_path=zip_filepath,
        include_base_folder=True,
        archive_exclude_patterns=[".lock", ".zip"],
    )
    arcproc.dataset.delete(geodatabase_path, log_level=logging.DEBUG)
    # pylint: disable=no-member
    geodatabase = site.content.add(
        item_properties={"type": "File Geodatabase", "tags": tags},
        # arcgis1.9.1: Convert to str.
        data=str(zip_filepath),
        folder=folder_name,
    )
    # pylint: enable=no-member
    zip_filepath.unlink()
    LOG.info("End: Upload.")
    return geodatabase
