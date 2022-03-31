"""Media (images, documents) processing objects."""
from collections import Counter
from datetime import datetime as _datetime
from logging import INFO, Logger, getLogger
from pathlib import Path
from subprocess import check_call
from time import sleep
from typing import Iterable, List, Optional, Sequence, Union

import img2pdf
from PIL import Image, ImageFile, ImageSequence

from pdfid_PL import PDFiD as pdfid

from proctools.misc import elapsed, log_entity_states


__all__ = []

LOG: Logger = getLogger(__name__)
"""Module-level logger."""

IMAGE_FILE_EXTENSIONS: List[str] = [
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
"""Collection of known image file extensions."""
WORLD_FILE_EXTENSIONS: List[str] = [
    ".j2w",
    ".jgw",
    ".jpgw",
    ".pgw",
    ".pngw",
    ".tfw",
    ".tifw",
    ".wld",
]
"""Collection of known image world file extensions."""


def _cmd_convert_image_to_pdf(
    image_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    error_on_failure: bool = False,
) -> str:
    """Convert image file to a PDF file using command-line tool.

    Args:
        image_path: Path to image file.
        output_path: Path to created PDF file.
        error_on_failure: Raise IOError if failure creating PDF.

    Returns:
        Result key--"converted", "failed to convert", or "no conversion necessary".

    Raises:
        IOError: If failure creating PDF and `error_on_failure = True`.
    """
    image_path = Path(image_path)
    output_path = Path(output_path)
    image2pdf_path: Path = (
        Path(__file__).parent.parent / "resources\\apps\\Image2PDF\\image2pdf.exe"
    )
    check_call(
        f"{image2pdf_path} -r EUIEUFBFYUOQVPAT"
        f""" -i "{image_path}" -o "{output_path}" -g overwrite"""
    )
    # Image2PDF returns before the process of the underlying library completes. So we
    # will need to wait until the PDF shows up in the file system.
    wait_seconds, max_seconds_waitedt, seconds_waited = 0.1, 30.0, 0.0
    while not output_path.is_file():
        if seconds_waited < max_seconds_waitedt:
            seconds_waited += wait_seconds
            sleep(wait_seconds)
        elif error_on_failure:
            raise IOError("Image2PDF failed to create PDF.")

        else:
            result = "failed to convert"
            break

    else:
        result = "converted"
    return result


def clean_pdf(
    pdf_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    overwrite_older_only: bool = True,
) -> str:
    """Clean PDF file free of scripting.

    Args:
        pdf_path: Path to PDF file.
        output_path: Path to cleaned PDF file.
        overwrite_older_only: If True and PDF already exists, will only overwrite if
            modified date is older than source file.

    Returns:
        Result key--"cleaned", "failed to clean", "no scripting to clean", or "no
        cleaning necessary".

    Raises:
        FileNotFoundError: If PDF file not an extant file.
    """
    pdf_path = Path(pdf_path)
    output_path = Path(output_path)
    if not pdf_path.is_file():
        raise FileNotFoundError(f"PDF file '{pdf_path}` not extant file.")

    if output_path.exists():
        if (
            overwrite_older_only
            and output_path.stat().st_mtime > pdf_path.stat().st_mtime
        ):
            return "no cleaning necessary"

    try:
        _, cleaned = pdfid(
            file=pdf_path, disarm=True, output_file=output_path, return_cleaned=True
        )
    # I believe this means there is no header with JavaScript in it.
    except UnboundLocalError:
        cleaned = None
    if cleaned is None:
        result = "no scripting to clean"
    elif cleaned:
        LOG.warning("`%s` had active content--cleaned.", pdf_path.name)
        result = "cleaned"
    else:
        output_path.unlink()
        result = "failed to clean"
    return result


def clean_pdfs(
    pdf_paths: Iterable[Union[Path, str]],
    *,
    overwrite_older_only: bool = True,
    logger: Optional[Logger] = None,
    log_evaluated_division: Optional[int] = None,
) -> Counter:
    """Clean PDF files of executable scripting.

    Args:
        pdf_paths: Paths to PDF files.
        overwrite_older_only: If True and PDF already exists, will only overwrite if
            modified date is older than source file.
        logger: Logger to emit loglines to. If set to None, will default to submodule
            logger.
        log_evaluated_division: Division at which to emit a logline about the number of
            files evaluated so far. If set to None, will default to not logging
            divisions.

    Returns:
        File counts for each clean result type.
    """
    start_time = _datetime.now()
    if logger is None:
        logger = LOG
    logger.info("Start: Clean PDFs.")
    states = Counter()
    for i, pdf_path in enumerate(pdf_paths, start=1):
        pdf_path = Path(pdf_path)
        cleaned_path = pdf_path.parent / ("Cleaned_" + pdf_path.name)
        result = clean_pdf(
            pdf_path,
            output_path=cleaned_path,
            overwrite_older_only=overwrite_older_only,
        )
        states[result] += 1
        if result == "cleaned":
            # Replace original with now-cleaned one.
            pdf_path.unlink()
            cleaned_path.rename(pdf_path)
        if log_evaluated_division and i % log_evaluated_division == 0:
            logger.info("Evaluated %s PDFs.", format(i, ",d"))
    log_entity_states("PDFs", states, logger=logger, log_level=INFO)
    elapsed(start_time, logger=logger)
    logger.info("End: Clean.")
    return states


def convert_image_to_pdf(
    image_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    disable_max_image_pixels: bool = False,
    overwrite_older_only: bool = True,
) -> str:
    """Convert image file to a PDF file.

    Args:
        image_path: Path to image file.
        output_path: Path to created PDF file.
        disable_max_image_pixels: If True, will disable the underlying library's maximum
            number of pixels an image can have to be processed.
        overwrite_older_only: If True and PDF already exists, will only overwrite if
            modified date is older than source file.

    Returns:
        Result key--"converted", "failed to convert", or "no conversion necessary".

    Raises:
        FileNotFoundError: If image file is not an extant file.
    """
    image_path = Path(image_path)
    output_path = Path(output_path)
    if not image_path.is_file():
        raise FileNotFoundError(f"Image file '{image_path}` not extant file.")

    if output_path.exists():
        if (
            overwrite_older_only
            and output_path.stat().st_mtime > image_path.stat().st_mtime
        ):
            return "no conversion necessary"

    # img2pdf uses Pillow, which will error out if the image in question exceeds
    # MAX_IMAGE_PIXELS with `PIL.Image.DecompressionBombError`. Can disable.
    if disable_max_image_pixels:
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
                result = _cmd_convert_image_to_pdf(image_path, output_path=output_path)
            else:
                raise

    return result


def convert_images_to_pdf(
    image_paths: Iterable[Union[Path, str]],
    *,
    disable_max_image_pixels: bool = False,
    overwrite_older_only: bool = True,
    logger: Optional[Logger] = None,
    log_evaluated_division: Optional[int] = None,
) -> Counter:
    """Convert image files to PDF files.

    Args:
        image_paths: Paths to image files.
        disable_max_image_pixels: If True, will disable the underlying library's maximum
            number of pixels an image can have to be processed.
        overwrite_older_only: If True and PDF already exists, will only overwrite if
            modified date is older than source file.
        logger: Logger to emit loglines to. If set to None, will default to submodule
            logger.
        log_evaluated_division: Division at which to emit a logline about the number of
            files evaluated so far. If set to None, will default to not logging
            divisions.

    Returns:
        File counts for each conversion result type.
    """
    start_time = _datetime.now()
    if logger is None:
        logger = LOG
    logger.info("Start: Convert images to PDFs.")
    states = Counter()
    for i, image_path in enumerate(image_paths, start=1):
        image_path = Path(image_path)
        result = convert_image_to_pdf(
            image_path,
            output_path=image_path.with_suffix(".pdf"),
            disable_max_image_pixels=disable_max_image_pixels,
            overwrite_older_only=overwrite_older_only,
        )
        states[result] += 1
        if log_evaluated_division and i % log_evaluated_division == 0:
            logger.info("Evaluated %s PDFs.", format(i, ",d"))
    log_entity_states("images", states, logger=logger, log_level=INFO)
    elapsed(start_time, logger=logger)
    logger.info("End: Convert.")
    return states


def convert_image_to_thumbnail(
    image_path: Union[Path, str],
    *,
    output_path: Union[Path, str],
    pixel_height: int,
    pixel_width: int,
    disable_max_image_pixels: bool = False,
    fallback_dpi: int = 72,
    overwrite_older_only: bool = True,
    resample: int = Image.BICUBIC,
) -> str:
    """Convert thumbnail image file of image file.

    Args:
        image_path: Path to image file.
        output_path: Path to created thumbnail file.
        pixel_height: Maximum height of thumbnail in pixels.
        pixel_width: Maximum width of thumbnail in pixels.
        disable_max_image_pixels: If True, will disable the underlying library's maximum
            number of pixels an image can have to be processed.
        fallback_dpi: Dots per inch (DPI) setting to use for thumbnail if source image
            does not have DPI information in header.
        overwrite_older_only: If True and thumbnail file already exists, will only
            overwrite if modified date is older than source file.
        resample: Filter to use for resampling. Refer to Pillow package for filter
            number codes.

    Returns:
        Result key--"converted", "failed to convert", or "no conversion necessary".

    Raises:
        FileNotFoundError: If image file is not an extant file.
    """
    image_path = Path(image_path)
    output_path = Path(output_path)
    if not image_path.is_file():
        raise FileNotFoundError(f"Image file '{image_path}` not extant file.")

    if output_path.exists():
        if (
            overwrite_older_only
            and output_path.stat().st_mtime > image_path.stat().st_mtime
        ):
            return "no conversion necessary"

    # img2pdf uses Pillow, which will error out if the image in question exceeds
    # MAX_IMAGE_PIXELS with `PIL.Image.DecompressionBombError`. Can disable.
    if disable_max_image_pixels:
        Image.MAX_IMAGE_PIXELS = None
    with Image.open(image_path) as image:
        try:
            image.thumbnail(size=(pixel_width, pixel_height), resample=resample)
        except IOError:
            # Attempt again but allow truncated images.
            # Alternative if necessary: https://stackoverflow.com/a/20068394
            ImageFile.LOAD_TRUNCATED_IMAGES = True
            try:
                image.thumbnail(size=(pixel_width, pixel_height), resample=resample)
            except IOError as error:
                raise IOError(f"image_path=`{image_path}`") from error

            finally:
                ImageFile.LOAD_TRUNCATED_IMAGES = False
        image.save(output_path, dpi=image.info.get("dpi", (fallback_dpi, fallback_dpi)))
    result = "converted"
    return result


def convert_images_to_thumbnails(
    image_paths: Iterable[Union[Path, str]],
    *,
    pixel_height: int,
    pixel_width: int,
    suffix: str,
    ignore_suffix: bool = True,
    disable_max_image_pixels: bool = False,
    overwrite_older_only: bool = True,
    resample: int = Image.BICUBIC,
    logger: Optional[Logger] = None,
    log_evaluated_division: Optional[int] = None,
) -> Counter:
    """Convert image files to thumbnail image files.

    Args:
        image_paths: Paths to image files.
        pixel_height: Maximum height of thumbnail in pixels.
        pixel_width: Maximum width of thumbnail in pixels.
        suffix: Suffix to attach to file name.
        ignore_suffix: If True & image file has the given suffix, ignore as an existing
            thumbnail.
        disable_max_image_pixels: If True, will disable the underlying library's maximum
            number of pixels an image can have to be processed.
        overwrite_older_only: If True and PDF already exists, will only overwrite if
            modified date is older than source file.
        resample: Filter to use for resampling. Refer to Pillow package for filter
            number codes.
        logger: Logger to emit loglines to. If set to None, will default to submodule
            logger.
        log_evaluated_division: Division at which to emit a logline about the number of
            files evaluated so far. If set to None, will default to not logging
            divisions.

    Returns:
        File counts for each conversion result type.
    """
    start_time = _datetime.now()
    if logger is None:
        logger = LOG
    logger.info("Start: Convert images to thumbnails.")
    states = Counter()
    for i, image_path in enumerate(image_paths, start=1):
        image_path = Path(image_path)
        if ignore_suffix and image_path.stem.casefold().endswith(suffix.casefold()):
            result = "ignoring for suffix"
        else:
            result = convert_image_to_thumbnail(
                image_path,
                output_path=image_path.stem + suffix + image_path.suffix,
                pixel_height=pixel_height,
                pixel_width=pixel_width,
                disable_max_image_pixels=disable_max_image_pixels,
                overwrite_older_only=overwrite_older_only,
                resample=resample,
            )
        states[result] += 1
        if log_evaluated_division and i % log_evaluated_division == 0:
            logger.info("Evaluated %s images.", format(i, ",d"))
    log_entity_states("images", states, logger=logger, log_level=INFO)
    elapsed(start_time, logger=logger)
    logger.info("End: Convert.")
    return states


def merge_tiffs(
    image_paths: Sequence[Union[Path, str]],
    *,
    output_path: Union[Path, str],
    disable_max_image_pixels: bool = False,
) -> str:
    """Merge sequence of TIFF image files into a single TIFF with multiple frames.

    Args:
        image_paths: Sequence of paths to TIFF image files.
        output_path: Path to merged TIFF image file.
        disable_max_image_pixels: If True, will disable the underlying library's maximum
            number of pixels an image can have to be processed.

    Returns:
        Result key--"merged", "failed to merge", or "no merge necessary".

    Raises:
        FileNotFoundError: If image file is not an extant file.
        OverflowError: If a frame in an image is corrupted or too large.
    """
    output_path = Path(output_path)
    # img2pdf uses Pillow, which will error out if the image in question exceeds
    # MAX_IMAGE_PIXELS with `PIL.Image.DecompressionBombError`. Can disable.
    if disable_max_image_pixels:
        Image.MAX_IMAGE_PIXELS = None
    frames = []
    for image_path in image_paths:
        image_path = Path(image_path)
        if not image_path.is_file():
            raise FileNotFoundError(f"Image file '{image_path}` not extant file.")

        with Image.open(image_path) as image:
            for i, frame in enumerate(ImageSequence.Iterator(image), start=1):
                try:
                    frames.append(frame.copy())
                except OverflowError as error:
                    raise OverflowError(
                        f"Frame {i} of `{image_path}` corrupted or too large"
                    ) from error

    frames[0].save(output_path, save_all=True, append_images=frames[1:])
    result = "merged"
    return result
