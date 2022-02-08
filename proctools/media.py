"""Media (images, documents) processing objects."""
from collections import Counter
import datetime
import logging
from pathlib import Path
import subprocess
import time

import img2pdf
from PIL import Image, ImageFile, ImageSequence

from pdfid_PL import PDFiD as pdfid

from .filesystem import folder_filepaths  # pylint: disable=relative-beyond-top-level
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
IMAGE2PDF_PATH = (
    Path(__file__).parent.parent / "resources\\apps\\Image2PDF\\image2pdf.exe"
)
"""pathlib.Path: Path to Image2PDF command-line app."""
IMAGE2PDF_CODE = "EUIEUFBFYUOQVPAT"
"""str: Registration code for Image2PDF command."""
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
        folder_path (pathlib.Path, str): Path to folder.
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
    folder_path = Path(folder_path)
    kwargs.setdefault("log_evaluated_division", -1)
    log = kwargs.get("logger", LOG)
    log.info("Start: Clean PDFs in folder `%s`.", folder_path)
    if not folder_path.is_dir():
        raise FileNotFoundError(f"`{folder_path}` not accessible folder")

    filepaths = folder_filepaths(folder_path, top_level_only, file_extensions=[".pdf"])
    states = Counter()
    for i, filepath in enumerate(filepaths, start=1):
        cleaned_path = filepath.parent / ("Cleaned_" + filepath.name)
        result = clean_pdf(filepath, output_path=cleaned_path)
        states[result] += 1
        if result == "cleaned":
            # Replace original with now-cleaned one.
            filepath.unlink()
            cleaned_path.rename(filepath)
        if (
            kwargs["log_evaluated_division"] > 0
            and i % kwargs["log_evaluated_division"] == 0
        ):
            log.info(f"Evaluated {i:,} documents.")
    log_entity_states("documents", states, logger=log, log_level=logging.INFO)
    elapsed(start_time, logger=log)
    log.info("End: Clean.")
    return states


def clean_pdf(source_path, output_path, **kwargs):
    """Clean PDF file free of scripting.

    Args:
        source_path (pathlib.Path, str): Path to PDF file.
        output_path (pathlib.Path, str): Path for cleaned PDF to be created at.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        overwrite_older_only (bool): If PDF already exists, will only overwrite if
            modified date is older than for the source file. Default is `True`.

    Returns:
        str: Result key--"cleaned", "failed to clean", or "no cleaning necessary".
    """
    source_path = Path(source_path)
    output_path = Path(output_path)
    kwargs.setdefault("overwrite_older_only", True)
    if source_path.suffix.lower() != ".pdf":
        raise ValueError("File must have .pdf extension.")

    if kwargs["overwrite_older_only"] and output_path.exists():
        if output_path.stat().st_mtime > source_path.stat().st_mtime:
            return "no conversion necessary"

    try:
        _, cleaned = pdfid(
            file=source_path, disarm=True, output_file=output_path, return_cleaned=True
        )
    # I believe this means there is no header with JS in it.
    except UnboundLocalError:
        cleaned = None
    if cleaned is None:
        result = "no scripting to clean"
    elif cleaned:
        LOG.warning("`%s` had active content--cleaned.", source_path.name)
        result = "cleaned"
    else:
        output_path.unlink()
        result = "failed to clean"
    return result


def convert_image_to_pdf(image_path, output_path, error_on_failure=False):
    """Convert image to a PDF.

    Args:
        image_path (pathlib.Path, str): Path to image file to convert.
        output_path (pathlib.Path, str): Path for PDF to be created at.
        error_on_failure (bool): Raise IOError if failure creating PDF.

    Returns:
        str: Result key--"converted" or "failed to convert".
    """
    image_path = Path(image_path)
    output_path = Path(output_path)
    if image_path.suffix.lower() not in IMAGE_FILE_EXTENSIONS:
        raise ValueError("Image must have image file extension.")

    call_string = (
        f"{IMAGE2PDF_PATH} -r {IMAGE2PDF_CODE}"
        f""" -i "{image_path}" -o "{output_path}" -g overwrite"""
    )
    subprocess.check_call(call_string)
    # Image2PDF returns before the process of the underlying library completes. So we
    # will need to wait until the PDF shows up in the file system.
    wait_interval, max_wait, wait_time = 0.1, 30.0, 0.0
    while not output_path.is_file():
        if wait_time < max_wait:
            wait_time += wait_interval
            time.sleep(wait_interval)
        elif error_on_failure:
            raise IOError("Image2PDF failed to create PDF.")

        else:
            result = "failed to convert"
            break

    else:
        result = "converted"
    return result


def convert_image_to_pdf2(image_path, output_path, **kwargs):
    """Convert image to a PDF.

    Args:
        image_path (pathlib.Path, str): Path to image file to convert.
        output_path (pathlib.Path, str): Path for PDF to be created at.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        overwrite_older_only (bool): If PDF already exists, will only overwrite if
            modified date is older than for the source file. Default is `True`.
        disable_max_image_pixels: If True the underlying library maximum number of
            pixels an image can have to be processed. Default is `False`.

    Returns:
        str: Result key--"converted", "failed to convert", or "no conversion necessary".
    """
    image_path = Path(image_path)
    output_path = Path(output_path)
    kwargs.setdefault("overwrite_older_only", True)
    kwargs.setdefault("disable_max_image_pixels", False)
    if image_path.suffix.lower() not in IMAGE_FILE_EXTENSIONS:
        raise ValueError("Image must have image file extension.")

    if kwargs["overwrite_older_only"] and output_path.exists():
        if output_path.stat().st_mtime > image_path.stat().st_mtime:
            return "no conversion necessary"

    # img2pdf uses Pillow, which will error out if the image in question exceeds
    # MAX_IMAGE_PIXELS with `PIL.Image.DecompressionBombError`. Can disable.
    if kwargs["disable_max_image_pixels"]:
        Image.MAX_IMAGE_PIXELS = None
    image_file = image_path.open(mode="rb")
    output_file = output_path.open(mode="wb")
    with image_file, output_file:
        try:
            pdf = img2pdf.convert(image_file)
            output_file.write(pdf)
            result = "converted"
        # Blame that alpha channel exception for the broad-except.
        except (TypeError, Exception) as error:  # pylint: disable=broad-except
            # img2pdf will not strip alpha channel (PDF images cannot have alphas).
            if str(error) == "Refusing to work on images with alpha channel":
                # The image2pdf command-line tool will do this.
                result = convert_image_to_pdf_cmd(image_path, output_path)
            # TODO: Strip out this commented-out code if Py3 does not have this problem.
            # # Value too large for long. Seems to be an issue with signed integers. Py2?
            # elif str(error).startswith("cannot handle type <type 'long'> with content"):
            #     result = convert_image_to_pdf_cmd(image_path, output_path)
            else:
                raise

    return result


def convert_image_to_pdf_cmd(image_path, output_path, error_on_failure=False):
    """Convert image to a PDF using command-line tool.

    Args:
        image_path (pathlib.Path, str): Path to image file to convert.
        output_path (pathlib.Path, str): Path for PDF to be created at.
        error_on_failure (bool): Raise IOError if failure creating PDF.

    Returns:
        str: Result key--"converted" or "failed to convert".
    """
    image_path = Path(image_path)
    output_path = Path(output_path)
    if image_path.suffix.lower() not in IMAGE_FILE_EXTENSIONS:
        raise ValueError("Image must have image file extension.")

    call_string = (
        f"{IMAGE2PDF_PATH} -r {IMAGE2PDF_CODE}"
        f""" -i "{image_path}" -o "{output_path}" -g overwrite"""
    )
    subprocess.check_call(call_string)
    # Image2PDF returns before the process of the underlying library completes. So we
    # will need to wait until the PDF shows up in the file system.
    wait_interval, max_wait, wait_time = 0.1, 30.0, 0.0
    while not output_path.is_file():
        if wait_time < max_wait:
            wait_time += wait_interval
            time.sleep(wait_interval)
        elif error_on_failure:
            raise IOError("Image2PDF failed to create PDF.")

        else:
            result = "failed to convert"
            break

    else:
        result = "converted"
    return result


def convert_folder_images_to_pdf(folder_path, top_level_only=False, **kwargs):
    """Convert image files to PDFs in folder.

    Args:
        folder_path (pathlib.Path, str): Path to folder.
        top_level_only (bool): Only update files at top-level of folder if True; include
            subfolders as well if False.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        keep_source_files (bool): Keep source image files if True, delete them if False.
            Default is True.
        overwrite_older_only (bool): If PDF already exists, will only overwrite if
            modified date is older than for the source file. Default is `True`.
        skip_suffixes (iter of str): Collection of strings to match against
            filename suffixes to ignore.
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
    folder_path = Path(folder_path)
    kwargs.setdefault("keep_source_files", True)
    kwargs.setdefault("overwrite_older_only", True)
    kwargs.setdefault("skip_suffixes", [])
    kwargs.setdefault("log_evaluated_division", -1)
    kwargs.setdefault("disable_max_image_pixels", False)
    log = kwargs.get("logger", LOG)
    log.info("Start: Convert image files to PDF in folder `%s`.", folder_path)
    if not folder_path.is_dir():
        raise FileNotFoundError(f"`{folder_path}` not accessible folder")

    filepaths = folder_filepaths(
        folder_path, top_level_only, file_extensions=IMAGE_FILE_EXTENSIONS
    )
    states = Counter()
    for i, filepath in enumerate(filepaths, start=1):
        result = None
        for suffix in kwargs["skip_suffixes"]:
            if suffix.lower() in filepath.name.lower():
                result = "skipped"
                continue

        if not result:
            output_filepath = filepath.with_suffix(".pdf")
            result = convert_image_to_pdf2(
                filepath,
                output_filepath,
                overwrite_older_only=kwargs["overwrite_older_only"],
                disable_max_image_pixels=kwargs["disable_max_image_pixels"],
            )
        states[result] += 1
        if not kwargs["keep_source_files"] and "failed" not in result:
            filepath.unlink()
        if (
            kwargs["log_evaluated_division"] > 0
            and i % kwargs["log_evaluated_division"] == 0
        ):
            log.info("Evaluated {:,} images.".format(i))
    log_entity_states("images", states, logger=log, log_level=logging.INFO)
    elapsed(start_time, logger=log)
    log.info("End: Convert.")
    return states


def create_folder_image_thumbnails(
    folder_path, width, height, suffix, top_level_only=False, **kwargs
):
    """Create thumbnails of images in folder.

    Args:
        folder_path (pathlib.Path, str): Path to folder.
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
        disable_max_image_pixels: If True the underlying library maximum number of
            pixels an image can have to be processed. Default is `False`.
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
    folder_path = Path(folder_path)
    kwargs.setdefault("resample", Image.BICUBIC)
    kwargs.setdefault("overwrite_older_only", True)
    kwargs.setdefault("ignore_suffix", True)
    kwargs.setdefault("disable_max_image_pixels", False)
    kwargs.setdefault("log_evaluated_division", -1)
    log = kwargs.get("logger", LOG)
    log.info("Start: Create thumbnail files for images in folder `%s`.", folder_path)
    if not folder_path.is_dir():
        raise FileNotFoundError(f"`{folder_path}` not accessible folder")

    filepaths = folder_filepaths(
        folder_path, top_level_only, file_extensions=IMAGE_FILE_EXTENSIONS
    )
    states = Counter()
    for i, filepath in enumerate(filepaths, start=1):
        # image_path_no_extension, extension = os.path.splitext(image_path)
        if kwargs["ignore_suffix"] and filepath.stem.lower().endswith(suffix.lower()):
            result = "ignoring for suffix"
        else:
            output_filepath = filepath.stem + suffix + filepath.suffix
            result = create_image_thumbnail(
                filepath,
                output_filepath,
                width,
                height,
                resample=kwargs["resample"],
                overwrite_older_only=kwargs["overwrite_older_only"],
                disable_max_image_pixels=kwargs["disable_max_image_pixels"],
            )
        states[result] += 1
        if (
            kwargs["log_evaluated_division"] > 0
            and i % kwargs["log_evaluated_division"] == 0
        ):
            log.info("Evaluated {:,} images.".format(i))
    log_entity_states("images", states, logger=log, log_level=logging.INFO)
    elapsed(start_time, logger=log)
    log.info("End: Create.")
    return states


def create_image_thumbnail(image_path, output_path, width, height, **kwargs):
    """Create a thumbnail of an image.

    Args:
        image_path (pathlib.Path, str): Path to image file to convert.
        output_path (pathlib.Path, str): Path for PDF to be created at.
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
    image_path = Path(image_path)
    output_path = Path(output_path)
    kwargs.setdefault("resample", Image.BICUBIC)
    kwargs.setdefault("overwrite_older_only", True)
    kwargs.setdefault("disable_max_image_pixels", False)
    fallback_dpi = (kwargs.get("fallback_dpi", 72), kwargs.get("fallback_dpi", 72))
    if image_path.suffix.lower() not in IMAGE_FILE_EXTENSIONS:
        raise ValueError("Image must have image file extension.")

    if kwargs["overwrite_older_only"] and output_path.exists():
        if output_path.stat().st_mtime > image_path.stat().st_mtime:
            return "no creation necessary"

    # img2pdf uses Pillow, which will error out if the image in question exceeds
    # MAX_IMAGE_PIXELS with `PIL.Image.DecompressionBombError`. Can disable.
    if kwargs["disable_max_image_pixels"]:
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
    result = "created"
    return result


def merge_tiffs(image_paths, output_path, **kwargs):
    """Merge a collection of TIFFs into a single TIFF with multiple frames.

    Args:
        image_paths (Sequence of pathlib.Path or str): Ordered collection of paths to
            TIFF image files to merge.
        output_path (pathlib.Path, str): Path for PDF to be created at.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        overwrite_older_only (bool): If output image already exists, will only overwrite
            if modified date is older than at least one source file. Default is `True`.
        disable_max_image_pixels: If True the underlying library maximum number of
            pixels an image can have to be processed. Default is `False`.

    Returns:
        str: Result key--"created", "failed to create", or "no creation necessary".
    """
    image_paths = [Path(filepath) for filepath in image_paths]
    output_path = Path(output_path)
    kwargs.setdefault("overwrite_older_only", True)
    kwargs.setdefault("disable_max_image_pixels", False)
    if any(
        image_path.suffix.lower() not in [".tif", ".tiff"] for image_path in image_paths
    ):
        raise ValueError("Image files must have TIFF file extension.")

    if kwargs["overwrite_older_only"] and output_path.exists():
        if all(
            output_path.stat().st_mtime > image_path.stat().st_mtime
            for image_path in image_paths
        ):
            return "no conversion necessary"

    if kwargs["disable_max_image_pixels"]:
        Image.MAX_IMAGE_PIXELS = None
    frames = []
    for image_path in image_paths:
        with Image.open(image_path) as image:
            for i, frame in enumerate(ImageSequence.Iterator(image), start=1):
                try:
                    frames.append(frame.copy())
                except OverflowError:
                    LOG.error("Frame %s of `%s` corrupted or too large", i, image_path)
                    raise

    frames[0].save(output_path, save_all=True, append_images=frames[1:])
    result = "converted"
    return result
