"""
PDF reference extraction using Grobid.

Note that this module depends on the ``grobid_client_python`` package
(https://github.com/kermitt2/grobid_client_python), which is not on PyPI. There
are several other Grobid client packages around, some with the same module name,
but they don't seem able to meet our needs.

Grobid extraction is done by communicating with a server. The environment
variable ``ADS_ARXIVREFS_GROBID_SERVER`` should give a base URL configuring how
the server is contacted. The default value is ``"http://localhost:8070"``.
"""

__all__ = ["extract_references"]

import argparse
from grobid_client.grobid_client import GrobidClient
from pathlib import Path
from typing import Generator, Tuple
from xml.etree import ElementTree as etree

from .compat import CompatExtractor
from .settings import Settings


__all__ = ["extract_references"]


def extract_references(
    session: CompatExtractor,
    pdf_path: Path,
    tr_path: Path,
    bibcode: str,
) -> int:
    """
    Extract references from a PDF file using GROBID.

    Parameters
    ----------
    session : CompatExtractor
        The extraction session object
    pdf_path : Path
        The absolute path of the PDF file
    tr_path : Path
        The absolute path of the target output references file
    bibcode : str
        The bibcode associated with the ArXiv submission

    Returns
    -------
    If a nonnegative integer, the number of references extracted. This indicates
    successful extraction. If the return value is a negative integer, extraction
    failed.
    """
    http_status, result = _call_grobid(pdf_path, session.settings)

    if http_status != 200:
        session.item_warn("GROBID returned an error code", code=http_status)
        session.item_give_up("grobid-error")
        return -1

    if not result:
        session.item_warn("GROBID returned an empty result", code=http_status)
        session.item_give_up("grobid-error")
        return -1

    n = 0

    with tr_path.open("wt") as f:
        print(f"%R {bibcode}", file=f)
        print("%Z", file=f)

        for ref in _get_refstrings(result):
            print(ref, file=f)
            n += 1

    return n


def _call_grobid(pdf_path: Path, settings: Settings) -> Tuple[int, str]:
    client = GrobidClient(
        check_server=False,
        grobid_server=settings.grobid_server,
    )
    _pdf_path, http_status, result = client.process_pdf(
        "processReferences",
        str(pdf_path),
        False,  # generateIDs
        False,  # consolidateHeader
        False,  # consolidateCitations
        True,  # includeRawCitations
        False,  # includeRawAffiliations
        None,  # teiCoordinates
        False,  # segmentSentences
    )
    return http_status, result


def _get_refstrings(result: str) -> Generator[str, None, None]:
    elem = etree.fromstring(result)

    for note in elem.iter("{http://www.tei-c.org/ns/1.0}note"):
        if note.attrib.get("type") == "raw_reference":
            text = note.text

            # Hyphenation normalization helps:
            text = text.replace("- ", "")

            yield text


# Diagnostic CLI


def _do_oneoff(settings):
    settings = Settings.new_defaults()

    pdf_path = settings.fulltext_base / f"{settings.item}.pdf"
    if not pdf_path.exists():
        raise Exception(f"no such file `{pdf_path}`")

    http_status, result = _call_grobid(pdf_path, settings)

    if http_status != 200:
        raise Exception(f"GROBID returned status {http_status}, not 200 as expected")

    for ref in _get_refstrings(result):
        print(ref)


def entrypoint():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="subcommand")

    p = commands.add_parser("oneoff")
    p.add_argument("item", metavar="ITEM", help="An ArXiv item ID")

    settings = parser.parse_args()
    if settings.subcommand is None:
        raise Exception("use a subcommand: oneoff")

    if settings.subcommand == "oneoff":
        _do_oneoff(settings)
    else:
        raise Exception(
            f"unknown subcommand `{settings.subcommand}`; run without arguments for a list"
        )


if __name__ == "__main__":
    entrypoint()
