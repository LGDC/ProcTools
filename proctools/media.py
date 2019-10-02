"""Media (images, documents) processing objects."""
from collections import Counter
import datetime
import logging
import os
import subprocess
import time

from .filesystem import folder_file_paths
from .misc import elapsed, log_entity_states


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

IMAGE_FILE_EXTENSIONS = [
    ".bmp",
    ".dcx",
    ".emf",
    ".gif",
    ".jpg",
    ".jpeg",
    ".pcd",
    ".pcx",
    ".pic",
    ".png",
    ".psd",
    ".tga",
    ".tif",
    ".tiff",
    ".wmf",
]
"""list of str: Collection of known image file extensions."""
IMAGE2PDF_PATH = os.path.join(
    os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir)),
    "resources\\apps\\Image2PDF\\image2pdf.exe -r EUIEUFBFYUOQVPAT",
)
"""str: Path to Image2PDF command-line app."""


def convert_image_to_pdf(image_path, output_path, error_on_failure=False):
    """Convert image to a PDF.

    Args:
        image_path (str): Path to image file to convert.
        output_path (str): Path for PDF to be created at.
        error_on_failure (bool): Raise IOError if failure creating PDF.

    Returns:
        str: Result key--"converted" or "failed to convert".
    """
    if os.path.splitext(image_path)[1].lower() not in IMAGE_FILE_EXTENSIONS:
        raise ValueError("Image must have image file extension.")

    subprocess.check_call(
        "{} -i {} -o {} -g overwrite".format(IMAGE2PDF_PATH, image_path, output_path)
    )
    # Image2PDF returns before the underlying library's process completes. So we will
    # need to wait until the PDF shows up in the file system.
    wait_interval, max_wait, wait_time = 0.1, 30.0, 0.0
    while os.path.isfile(output_path) is False:
        if wait_time < max_wait:
            wait_time += wait_interval
            time.sleep(wait_interval)
        elif error_on_failure:
            raise IOError("Image2PDF failed to create PDF.")

        else:
            result_key = "failed to convert"
            break

    else:
        result_key = "converted"
    return result_key


def convert_folder_images_to_pdf(
    folder_path, keep_source_files=True, top_level_only=False, **kwargs
):
    """Convert image files i replica folder from source.

    Args:
        folder_path (str): Path to folder.
        keep_source_files (bool): Keep source image files if True, delete them if False.
        top_level_only (bool): Only update files at top-level of folder if True; include
            subfolders as well if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        logger (logging.Logger): Logger to emit loglines to. If not defined will default
            to submodule logger.
        log_evaluated_division (int): Division at which to emit a logline about number
            of files evaluated so far. If not defined or None, will default to not
            logging evaluated divisions.

    Returns:
        collections.Counter: Counts for each update result type: "converted" or "failed
            to convert".
    """
    start_time = datetime.datetime.now()
    log = kwargs.get("logger", LOG)
    log.info("Start: Convert image files to PDF in folder `%s`.", folder_path)
    if not os.access(folder_path, os.R_OK):
        raise OSError("Cannot access `{}`.".format(folder_path))

    states = Counter()
    image_paths = folder_file_paths(
        folder_path, top_level_only, file_extensions=IMAGE_FILE_EXTENSIONS
    )
    for i, image_path in enumerate(image_paths, start=1):
        output_path = os.path.splitext(image_path)[0] + ".pdf"
        states[convert_image_to_pdf(image_path, output_path)] += 1
        if not keep_source_files:
            os.remove(image_path)
        if "log_evaluated_division" in kwargs:
            if i % kwargs["log_evaluated_division"] == 0:
                log.info("Evaluated {:,} images.".format(i))
    log_entity_states("images", states, log)
    elapsed(start_time, log)
    log.info("End: Update.")
    return states
