"""
Miscellaneous utilities that don't fit anywhere else.
"""

__all__ = ["get_quick_logger", "split_item_path"]

import logging
import sys


def get_quick_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """
    Get a basic logger for CLI utilities.

    This prints to stderr, since the standard usage of this program needs to
    keep its stdout pristine.
    """

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s\t%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def split_item_path(item_path):
    """
    Convert an arxiv "item path" to its "stem" and "extension".

    The item path may be either a string of the form "arXiv/2110/08013.tar.gz",
    or the same with a filesystem prefix: "/proj/ads/.../fulltext/arXiv/...".
    The extension is not necessarily ".tar.gz"

    The "stem" of such a path is "arXiv/2110/08013", and the extension is
    "tar.gz". If there is no period in the basename of the item path, the
    extension is the empty string.
    """
    item_path = str(item_path)

    # Sometimes this is `arXiv/YYMM/NNNNN.EXT`, sometimes
    # `/proj/ads/.../fulltext/arXiv/YYMM/NNNNN.EXT`
    bits = item_path.split("fulltext/")
    if len(bits) > 1:
        item_path = bits[-1]

    bits = item_path.split(".", 1)
    stem = bits[0]

    if len(bits) > 1:
        ext = bits[1]
    else:
        ext = ""

    return (stem, ext)
