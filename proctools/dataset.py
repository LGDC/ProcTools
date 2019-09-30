"""Dataset objects."""
import itertools
import logging
import sys

# import arcetl  # Imported locally to avoid slow imports.

# Py2.
if sys.version_info.major >= 3:
    basestring = str


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


class Dataset(object):
    """Representation of dataset information."""

    valid_geometry_types = ["point", "multipoint", "polygon", "polyline"]

    @staticmethod
    def init_tag_property(value):
        """Initialize a tag-style property mapping.

        Args:
            value (dict, str): Value to initialize into tag-property.

        Returns:
            dict
        """
        if isinstance(value, dict):
            return value

        if isinstance(value, basestring):
            return {None: value}

        raise TypeError("Invalid type for tag property.")

    def __init__(self, fields, geometry_type=None, path=None):
        """Initialize instance.

        Args:
            fields (iter): Collection of field information dictionaries.
            geometry_type (str, None): Type of geometry. NoneType indicates nonspatial.
            path (dict, str): Path or tagged mapping of paths.
        """
        self._geometry_type = None
        self.geometry_type = geometry_type
        self.fields = list(fields)
        self._path = self.init_tag_property(path)

    @property
    def field_names(self):
        """list of str: Dataset field names."""
        return [field["name"] for field in self.fields]

    @property
    def geometry_type(self):
        """str, None: Dataset geometry type."""
        return self._geometry_type

    @geometry_type.setter
    def geometry_type(self, value):
        if value is None:
            self._geometry_type = value
        elif value.lower() in Dataset.valid_geometry_types:
            self._geometry_type = value.lower()

    @property
    def id_field_names(self):
        """list of str: Dataset identifier field names."""
        return [field["name"] for field in self.fields if field.get("is_id")]

    @property
    def id_fields(self):
        """list of dict: Dataset identifier field info dictionaries."""
        return [field for field in self.fields if field["is_id"]]

    def add_path(self, tag, path):
        """Add a path for the given tag.

        Args:
            tag (str): Path tag.
            path (str): Path to add.
        """
        self._path[tag] = path

    def create(self, path, field_tag=None, spatial_reference_item=None):
        """Create dataset from instance properties.

        Args:
            path (str): Path for dataset to create.
            field_tag (str, None): Tag for fields to add to created dataset. If None,
                all fields listed in self.fields will be added.
            spatial_reference_item (object): Object with which to define the spatial
                reference from. If None, dataset created will be nonspatial.

        Returns:
            str: Path of dataset created.
        """
        import arcetl

        # Check for path in path-tags; otherwise assume path is literal.
        dataset_path = self._path.get(path, path)
        field_metadata_list = (
            [field for field in self.fields if field_tag in field["tags"]]
            if field_tag
            else self.fields
        )
        arcetl.dataset.create(
            dataset_path,
            field_metadata_list,
            geometry_type=self.geometry_type,
            spatial_reference_item=spatial_reference_item,
        )
        return dataset_path

    def path(self, tag=None):
        """Return path string associated with the given tag.

        Args:
            tag (str): Path tag.

        Returns:
            str: Path of dataset with given tag.
        """
        return self._path[tag]


def last_change_date(
    dataset_path, init_date_field_name="init_date", mod_date_field_name="mod_date"
):
    """Return date of the last change on dataset with tracking fields."""
    import arcetl

    field_names = [init_date_field_name, mod_date_field_name]
    date_iters = arcetl.attributes.as_iters(dataset_path, field_names)
    dates = set(itertools.chain.from_iterable(date_iters))
    # datetimes cannot compare to NoneTypes.
    if None in dates:
        dates.remove(None)
    return max(dates) if dates else None
