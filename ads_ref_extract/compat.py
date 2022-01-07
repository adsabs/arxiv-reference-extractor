"""
"Compatibility" mode for reference extraction in Python.

This file defines a CLI reference extractor that aims to be drop-in compatible
with the classic `extractrefs.pl` implementation. But better, hopefully!

Along with the command-line arguments, this program must be compatible regarding
the standard streams. Inputs are read off of stdin, which should be a sequence
of line-oriented records:

    FULLTEXT-PATH [BIBCODE ACCNO SUBDATE]

e.g.

    arXiv/2111/01106.tar.gz 2021arXiv211101106G     X18-80339       20211101
    arXiv/2111/01105.pdf    2021arXiv211101105M     X18-80338       20211101

The last three records are optional (but have to come as a group). The program's
standard output consists of lines with one or two items:

    FULLTEXT-PATH [TARGET-REFS-PATH]

e.g.

    /proj/ads/abstracts/sources/ArXiv/fulltext/arXiv/2111/01104.tex /proj/ads/references/sources/arXiv/2111/01104.raw
    arXiv/2111/01103.pdf

The presence of the second item indicates that references were successfully
extracted. Due to inconsistencies in how the Perl did its processing, sometimes
the FULLTEXT-PATH was absolutified, and sometimes not.
"""

import argparse
import logging
from pathlib import Path
import sys
from typing import Optional

from .config import Config
from .utils import split_item_path

__all__ = ["entrypoint", "CompatExtractor"]


default_logger = logging.getLogger("extractrefs")


class CompatExtractor(object):
    config: Config = None
    "A Config object with path configuration information"

    logger: logging.Logger = None
    "A logger"

    force = False
    "Recreate target reference file even if it exists and is more recent than source."

    no_harvest = False
    "Do not attempt to harvest or refresh PDF files from arXiv."

    no_pdf = False
    "Do not attempt to process PDF files if original source was LaTeX (implies `no_harvest`)."

    skip_refs = False
    "If true, don't actually write out the new ('target') reference file."

    @classmethod
    def new_from_commandline(cls, argv=sys.argv):
        parser = argparse.ArgumentParser(
            epilog="""The program reads from stdin a table consisting of the
fulltext e-print file (first column) and optionally its corresponding bibcode
(second column), accno number (third column), and submission date (fourth
column). If a bibcode is not given, one is obtained from bib2accno.list

The fulltext filenames typically are in one of these forms:
    arXiv/0705/0161.tar.gz
    arXiv/0705/0160.pdf
    math/2006/0604548.tex.gz
"""
        )
        parser.add_argument(
            "--pbase",
            metavar="PATH",
            help="Specify alternative base directory for fulltext source",
        )
        parser.add_argument(
            "--tbase",
            metavar="PATH",
            help="Specify alternative base directory for target ref files",
        )
        parser.add_argument(
            "--texbase",
            dest="texbindir",
            metavar="PATH",
            help="Specify alternative directory for TeX binaries (note: semantics changed from extractrefs.pl)",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force recreation of references even if target file exists and is more recent than source",
        )
        parser.add_argument(
            "--no-harvest",
            action="store_true",
            help="Do not attempt to harvest or refresh PDF files from arXiv",
        )
        parser.add_argument(
            "--no-pdf",
            action="store_true",
            help="Do not attempt to process PDF files if original source was LaTeX (implies --no-harvest)",
        )
        parser.add_argument(
            "--skip-refs",
            action="store_true",
            help="Perform processing but skip writing the references",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Print debugging information",
        )

        settings = parser.parse_args(argv[1:])

        # Set up logging ASAP. We need to configure to always go to stderr,
        # since our stdout is parsed.

        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s\t%(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        default_logger.addHandler(handler)

        if settings.debug:
            default_logger.setLevel(logging.DEBUG)
        else:
            default_logger.setLevel(logging.INFO)

        # Now let's do the Config ...

        config = Config.new_defaults()
        if settings.pbase is not None:
            config.fulltext_base = Path(settings.pbase)

        if settings.tbase is not None:
            config.target_refs_base = Path(settings.tbase)

        if (
            settings.texbindir is not None
        ):  # this is `--texbase`; our semantics are different
            config.tex_bin_dir = Path(settings.texbindir)

        # Now the rest.

        inst = cls()
        inst.config = config
        inst.logger = default_logger
        inst.force = settings.force
        inst.no_harvest = settings.no_harvest or settings.no_pdf
        inst.no_pdf = settings.no_pdf
        inst.skip_refs = settings.skip_refs
        return inst

    def process(self, stream=sys.stdin):
        self.logger.info("using the new Python extractrefs")
        n_inputs = 0
        n_failures = 0

        for line in stream:
            pieces = line.strip().split()
            if not pieces:
                self.logger.debug("ignoring blank input line")
                continue

            preprint_path = pieces[0]
            n_inputs += 1

            if len(pieces) > 3:
                # accno is not actually used in the processing
                # subdate was used to choose correct TeX stack to use - we don't
                # support that anymore
                bibcode, _accno, _subdate = pieces[1:4]
            else:
                bibcode = None

            target_ref_path = self.process_one(preprint_path, bibcode)

            if target_ref_path is None:
                print(preprint_path)
                n_failures += 1
            else:
                print(preprint_path, target_ref_path)

        self.logger.info(f"processed {n_inputs} items")
        if n_failures:
            self.logger.info(f"{n_failures} items could not be processed")
        return 0

    def process_one(self, preprint_path: str, bibcode: Optional[str]) -> Optional[str]:
        """
        Process a single preprint.

        Returns the path to the newly created reference file if the preprint was
        processed successfully. Returns None if it couldn't be processed.
        """
        # Check out the fulltext source

        item_stem, item_ext = split_item_path(preprint_path)

        item_id = item_stem  # might tweak this later
        ft_path = self.config.fulltext_base / f"{item_stem}.{item_ext}"

        if not ft_path.exists():
            self.logger.warning(
                f"{item_id}: cannot find expected file `{ft_path}` for input `{preprint_path}`"
            )
            return None

        if item_ext in ("tar.gz", "tar", "tex.gz", "tex", "gz"):
            is_pdf = False
        elif item_ext in ("pdf", "pdf.gz"):
            is_pdf = True
        else:
            self.logger.warning(
                f"{item_id}: unexpected extension `{item_ext}` for input `{preprint_path}`; ignoring"
            )
            return None

        # Check out the target refs file

        tr_path = self.config.target_refs_base / f"{item_stem}.raw"

        if not tr_path.exists():
            self.logger.debug(f"{item_id}: creating output {tr_path}")
        elif tr_path.stat().st_mtime < ft_path.stat().st_mtime:
            self.logger.debug(f"{item_id}: output {tr_path} needs updating")
        elif self.force:
            self.logger.debug(f"{item_id}: forcing recreation of output {tr_path}")
        else:
            self.logger.debug(f"{item_id}: output {tr_path} is up-to-date")
            return str(tr_path)

        # TODO: this is where classic guesses the bibcode and subdate if needed.
        if bibcode is None:
            self.logger.warning(f"{item_id}: TEMP bailing because no bibcode")
            return None

        wrote_refs = False

        if not is_pdf:
            try:
                from . import tex

                wrote_refs = tex.extract_references(
                    self, item_id, ft_path, tr_path, bibcode
                )
            except Exception as e:
                self.logger.warning(
                    f"{item_id}: TeX extraction failed: {e} ({e.__class__.__name__})"
                )

        if not wrote_refs:
            self.logger.warning(f"{item_id}: TEMP bailing: TeX didn't work")
            return None

        return tr_path


def entrypoint(argv=sys.argv, stream=sys.stdin):
    sys.exit(CompatExtractor.new_from_commandline(argv).process(stream))


if __name__ == "__main__":
    entrypoint()
