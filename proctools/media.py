"""Media (images, documents) processing objects."""
from collections import Counter
import datetime
import logging
import os
import shutil
import subprocess
import time

import img2pdf
from PIL import Image, ImageFile, ImageSequence

from pdfid_PL import PDFiD as pdfid

from .filesystem import folder_file_paths  # pylint: disable=relative-beyond-top-level
from .misc import (  # pylint: disable=relative-beyond-top-level
    elapsed,
    log_entity_states,
)


__all__ = []

LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

IMAGE_FILE_EXTENSIONS = [
    ".bmp",
    ".dcx",
    ".emf",
    ".gif",
    ".jp2",
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
WORLD_FILE_EXTENSIONS = [
    ".j2w",
    ".jgw",
    ".jpgw",
    ".pgw",
    ".pngw",
    ".tfw",
    ".tifw",
    ".wld",
]
"""list of str: Collection of known image world file extensions."""


def clean_folder_pdfs(folder_path, top_level_only=False, **kwargs):
    """Clean PDF files in folder of scripting.

    Args:
        folder_path (str): Path to folder.
        top_level_only (bool): Only clean files at top-level of folder if True; include
            subfolders as well if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        logger (logging.Logger): Logger to emit loglines to. If not defined will default
            to submodule logger.
        log_evaluated_division (int): Division at which to emit a logline about number
            of files evaluated so far. If not defined or None, will default to not
            logging evaluated divisions.

    Returns:
        collections.Counter: Counts for each update result type: "cleaned" or "failed to
        clean".
    """
    start_time = datetime.datetime.now()
    log = kwargs.get("logger", LOG)
    log.info("Start: Clean PDFs in folder `%s`.", folder_path)
    if not os.access(folder_path, os.R_OK):
        raise OSError("Cannot access `{}`.".format(folder_path))

    states = Counter()
    source_paths = folder_file_paths(
        folder_path, top_level_only, file_extensions=[".pdf"]
    )
    for i, source_path in enumerate(source_paths, start=1):
        root_name, extension = os.path.splitext(os.path.basename(source_path))
        cleaned_path = root_name + "_cleaned" + extension
        result_key = clean_pdf(source_path, cleaned_path)
        states[result_key] += 1
        if result_key == "cleaned":
            # Replace original with now-cleaned one.
            shutil.move(cleaned_path, source_path)
        if "log_evaluated_division" in kwargs:
            if i % kwargs["log_evaluated_division"] == 0:
                log.info("Evaluated {:,} documents.".format(i))
    log_entity_states("documents", states, log, log_level=logging.INFO)
    elapsed(start_time, log)
    log.info("End: Clean.")
    return states


def clean_pdf(source_path, output_path, **kwargs):
    """Clean PDF file free of scripting.

    Args:
        source_path (str): Path to PDF file.
        output_path (str): Path for cleaned PDF to be created at.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        overwrite_older_only (bool): If PDF already exists, will only overwrite if
            modified date is older than for the source file. Default is `True`.

    Returns:
        str: Result key--"cleaned", "failed to clean", or "no cleaning necessary".
    """
    if os.path.splitext(source_path)[1].lower() != ".pdf":
        raise ValueError("File must have .pdf extension.")

    if kwargs.get("overwrite_older_only", True) and os.path.exists(output_path):
        if os.path.getmtime(output_path) > os.path.getmtime(source_path):
            return "no conversion necessary"

    try:
        _, cleaned = pdfid(
            file=source_path, disarm=True, output_file=output_path, return_cleaned=True
        )
    # I believe this means there is no header with JS in it.
    except UnboundLocalError:
        cleaned = None
    if cleaned is None:
        result_key = "no scripting to clean"
    elif cleaned:
        LOG.warning("`%s` had active content--cleaned.", os.path.basename(source_path))
        result_key = "cleaned"
    else:
        os.remove(output_path)
        result_key = "failed to clean"
    return result_key


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

    call_string = """{} -i "{}" -o "{}" -g overwrite""".format(
        IMAGE2PDF_PATH, image_path, output_path
    )
    subprocess.check_call(call_string)
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


def convert_image_to_pdf2(image_path, output_path, **kwargs):
    """Convert image to a PDF.

    Args:
        image_path (str): Path to image file to convert.
        output_path (str): Path for PDF to be created at.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        overwrite_older_only (bool): If PDF already exists, will only overwrite if
            modified date is older than for the source file. Default is `True`.
        disable_max_image_pixels: If True the underlying library maximum number of
            pixels an image can have to be processed. Default is `False`.

    Returns:
        str: Result key--"converted", "failed to convert", or "no conversion necessary".
    """
    if os.path.splitext(image_path)[1].lower() not in IMAGE_FILE_EXTENSIONS:
        raise ValueError("Image must have image file extension.")

    if kwargs.get("overwrite_older_only", True) and os.path.exists(output_path):
        if os.path.getmtime(output_path) > os.path.getmtime(image_path):
            return "no conversion necessary"

    # img2pdf uses Pillow, which will error out if the image in question exceeds
    # MAX_IMAGE_PIXELS with `PIL.Image.DecompressionBombError`. Can disable.
    if kwargs.get("disable_max_image_pixels"):
        Image.MAX_IMAGE_PIXELS = None
    image_file = open(image_path, mode="rb")
    output_file = open(output_path, mode="wb")
    with image_file, output_file:
        try:
            pdf = img2pdf.convert(image_file)
            output_file.write(pdf)
            result_key = "converted"
        # Blame that alpha channel exception for the broad-except.
        except (TypeError, Exception) as error:  # pylint: disable=broad-except
            # Value too large for long. Seems to be an issue with signed integers. Py2?
            if str(error).startswith("cannot handle type <type 'long'> with content"):
                result_key = convert_image_to_pdf_cmd(image_path, output_path)
            # img2pdf will not strip alpha channel (PDF images cannot have alphas).
            elif str(error) == "Refusing to work on images with alpha channel":
                # The image2pdf command-line tool will do this.
                result_key = convert_image_to_pdf_cmd(image_path, output_path)
            else:
                raise

    return result_key


def convert_image_to_pdf_cmd(image_path, output_path, error_on_failure=False):
    """Convert image to a PDF using command-line tool.

    Args:
        image_path (str): Path to image file to convert.
        output_path (str): Path for PDF to be created at.
        error_on_failure (bool): Raise IOError if failure creating PDF.

    Returns:
        str: Result key--"converted" or "failed to convert".
    """
    if os.path.splitext(image_path)[1].lower() not in IMAGE_FILE_EXTENSIONS:
        raise ValueError("Image must have image file extension.")

    call_string = """{} -i "{}" -o "{}" -g overwrite""".format(
        IMAGE2PDF_PATH, image_path, output_path
    )
    subprocess.check_call(call_string)
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
    """Convert image files to PDFs in folder.

    Args:
        folder_path (str): Path to folder.
        keep_source_files (bool): Keep source image files if True, delete them if False.
        top_level_only (bool): Only update files at top-level of folder if True; include
            subfolders as well if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        skip_suffixes (iter of str): Collection of strings to match against
            filenames to ignore.
        overwrite_older_only (bool): If PDF already exists, will only overwrite if
            modified date is older than for the source file. Default is `True`.
        logger (logging.Logger): Logger to emit loglines to. If not defined will default
            to submodule logger.
        log_evaluated_division (int): Division at which to emit a logline about number
            of files evaluated so far. If not defined or None, will default to not
            logging evaluated divisions.
        disable_max_image_pixels: If True the underlying library maximum number of
            pixels an image can have to be processed. Default is `False`.

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
        result_key = None
        for suffix in kwargs.get("skip_suffixes", []):
            if suffix.lower() in os.path.basename(image_path).lower():
                result_key = "skipped"
                continue

        if not result_key:
            output_path = os.path.splitext(image_path)[0] + ".pdf"
            result_key = convert_image_to_pdf2(image_path, output_path, **kwargs)
        states[result_key] += 1
        if not keep_source_files and "failed" not in result_key:
            os.remove(image_path)
        if "log_evaluated_division" in kwargs:
            if i % kwargs["log_evaluated_division"] == 0:
                log.info("Evaluated {:,} images.".format(i))
    log_entity_states("images", states, log, log_level=logging.INFO)
    elapsed(start_time, log)
    log.info("End: Convert.")
    return states


def create_folder_image_thumbnails(
    folder_path, width, height, suffix, top_level_only=False, **kwargs
):
    """Create thumbnails of images in folder.

    Args:
        folder_path (str): Path to folder.
        width (int): Maximum width in pixels.
        height (int): Maximum height in pixels.
        suffix (str): Suffix to attach to file name.
        top_level_only (bool): Only update files at top-level of folder if True; include
            subfolders as well if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        resample (int): Filter to use for resampling. Refer to Pillow package for filter
            number codes. Default is Bicubic (PIL.Image.BICUBIC = 3)
        overwrite_older_only (bool): If PDF already exists, will only overwrite if
            modified date is older than for the source file. Default is `True`.
        ignore_suffix (bool): If image file has the given suffix, ignore as an existing
            thumbnail. Default is True.
        logger (logging.Logger): Logger to emit loglines to. If not defined will default
            to submodule logger.
        log_evaluated_division (int): Division at which to emit a logline about number
            of files evaluated so far. If not defined or None, will default to not
            logging evaluated divisions.
        disable_max_image_pixels: If True the underlying library maximum number of
            pixels an image can have to be processed. Default is `False`.

    Returns:
        collections.Counter: Counts for each update result type: "converted" or "failed
            to convert".
    """
    start_time = datetime.datetime.now()
    log = kwargs.get("logger", LOG)
    log.info("Start: Create thumbnail files for images in folder `%s`.", folder_path)
    if not os.access(folder_path, os.R_OK):
        raise OSError("Cannot access `{}`.".format(folder_path))

    states = Counter()
    image_paths = folder_file_paths(
        folder_path, top_level_only, file_extensions=IMAGE_FILE_EXTENSIONS
    )
    for i, image_path in enumerate(image_paths, start=1):
        image_path_no_extension, extension = os.path.splitext(image_path)
        if kwargs.get(
            "ignore_suffix", True
        ) and image_path_no_extension.lower().endswith(suffix.lower()):
            result_key = "ignoring for suffix"
        else:
            output_path = image_path_no_extension + suffix + extension
            result_key = create_image_thumbnail(
                image_path, output_path, width, height, **kwargs
            )
        states[result_key] += 1
        if "log_evaluated_division" in kwargs:
            if i % kwargs["log_evaluated_division"] == 0:
                log.info("Evaluated {:,} images.".format(i))
    log_entity_states("images", states, log, log_level=logging.INFO)
    elapsed(start_time, log)
    log.info("End: Create.")
    return states


def create_image_thumbnail(image_path, output_path, width, height, **kwargs):
    """Create a thumbnail of an image.

    Args:
        image_path (str): Path to image file to convert.
        output_path (str): Path for PDF to be created at.
        width (int): Maximum width in pixels.
        height (int): Maximum height in pixels.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        resample (int): Filter to use for resampling. Refer to Pillow package for filter
            number codes. Default is Bicubic (PIL.Image.BICUBIC = 3)
        fallback_dpi (int): DPI to use for thumbnail if source image does not have DPI
            information in header. Default is 72.
        overwrite_older_only (bool): If PDF already exists, will only overwrite if
            modified date is older than for the source file. Default is `True`.
        disable_max_image_pixels: If True the underlying library maximum number of
            pixels an image can have to be processed. Default is `False`.

    Returns:
        str: Result key--"created", "failed to create", or "no creation necessary".
    """
    kwargs.setdefault("resample", Image.BICUBIC)
    fallback_dpi = (kwargs.get("fallback_dpi", 72), kwargs.get("fallback_dpi", 72))
    if os.path.splitext(image_path)[1].lower() not in IMAGE_FILE_EXTENSIONS:
        raise ValueError("Image must have image file extension.")

    if kwargs.get("overwrite_older_only", True) and os.path.exists(output_path):
        if os.path.getmtime(output_path) > os.path.getmtime(image_path):
            return "no conversion necessary"

    # Pillow will error out if the image in question exceeds MAX_IMAGE_PIXELS with
    # `PIL.Image.DecompressionBombError`. Can disable.
    if kwargs.get("disable_max_image_pixels"):
        Image.MAX_IMAGE_PIXELS = None
    with Image.open(image_path) as image:
        try:
            image.thumbnail(size=(width, height), resample=kwargs["resample"])
        except IOError:
            # Attempt again but allow truncated images.
            # Alternative if necessary: https://stackoverflow.com/a/20068394
            ImageFile.LOAD_TRUNCATED_IMAGES = True
            try:
                image.thumbnail(size=(width, height), resample=kwargs["resample"])
            except IOError:
                LOG.exception("image_path=`%s`", image_path)
                raise

            finally:
                ImageFile.LOAD_TRUNCATED_IMAGES = False

        image.save(output_path, dpi=image.info.get("dpi", fallback_dpi))
    result_key = "converted"
    return result_key


def merge_tiffs(image_paths, output_path, **kwargs):
    """Merge a collection of TIFFs into a single TIFF with multiple frames.

    Args:
        image_paths (str): Ordered collection of paths to TIFF image files to merge.
        output_path (str): Path for PDF to be created at.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        overwrite_older_only (bool): If output image already exists, will only overwrite
            if modified date is older than at least one source file. Default is `False`.
        disable_max_image_pixels: If True the underlying library maximum number of
            pixels an image can have to be processed. Default is `False`.

    Returns:
        str: Result key--"created", "failed to create", or "no creation necessary".
    """
    if any(
        os.path.splitext(image_path)[1].lower() not in [".tif", ".tiff"]
        for image_path in image_paths
    ):
        raise ValueError("Images must have TIFF file extension.")

    if kwargs.get("overwrite_older_only", True) and os.path.exists(output_path):
        if all(
            os.path.getmtime(output_path) > os.path.getmtime(image_path)
            for image_path in image_paths
        ):
            return "no conversion necessary"

    if kwargs.get("disable_max_image_pixels"):
        Image.MAX_IMAGE_PIXELS = None
    frames = []
    for image_path in image_paths:
        with Image.open(image_path) as image:
            for i, frame in enumerate(ImageSequence.Iterator(image), start=1):
                try:
                    frames.append(frame.copy())
                except OverflowError:
                    LOG.error("Frame %s of %s corrupted or too large", i, image_path)
                    raise

    frames[0].save(output_path, save_all=True, append_images=frames[1:])
    result_key = "converted"
    return result_key
