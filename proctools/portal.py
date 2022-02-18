"""ArcGIS Online & other portal objects."""
import logging
from typing import Optional, Union

from arcgis.gis import GIS
from arcgis.features import FeatureLayer, Table


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
