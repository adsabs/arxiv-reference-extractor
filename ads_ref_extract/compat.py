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
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import List, Optional, TextIO, Union

from .config import Config
from .utils import split_item_path

__all__ = ["entrypoint", "CompatExtractor"]


default_logger = logging.getLogger("extractrefs")


class CompatExtractor(object):
    config: Config = None
    "A Config object with path configuration information"

    logger: logging.Logger = None
    "A logger"

    input_stream: TextIO = None

    output_stream: TextIO = None

    log_stream: TextIO = None

    force = False
    "Recreate target reference file even if it exists and is more recent than source."

    no_harvest = False
    "Do not attempt to harvest or refresh PDF files from arXiv."

    no_pdf = False
    "Do not attempt to process PDF files if original source was LaTeX (implies `no_harvest`)."

    no_tex = False
    "Do not attempt LaTeX processing."

    skip_refs = False
    "If true, don't actually write out the new ('target') reference file."

    debug_tex = False
    "If true, print TeX's output (which can be voluminous)."

    debug_source_files_dir: Optional[Path] = None
    "If set, a directory where unpacked/munged source files will be preserved."

    debug_pdftotext = False
    "If true, print pdftotext output."

    pdf_helper: Path = None

    @classmethod
    def new_from_commandline(cls, argv=sys.argv):
        parser = argparse.ArgumentParser(
            epilog="""(Original extractrefs.pl summary:) The program reads a table consisting of the
fulltext e-print file (first column) and optionally its corresponding bibcode
(second column), accno number (third column), and submission date (fourth
column). If a bibcode is not given, one is obtained from bib2accno.list

The fulltext filenames typically are in one of these forms:
    arXiv/0705/0161.tar.gz
    arXiv/0705/0160.pdf
    math/2006/0604548.tex.gz
""",
        )
        parser.add_argument(
            "--pipeline",
            metavar="PATH",
            help="Operate in pipeline mode, reading items from the specified path",
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
            "--no-tex",
            action="store_true",
            help="Do not attempt LaTeX processing",
        )
        parser.add_argument(
            "--skip-refs",
            action="store_true",
            help="Perform processing but skip writing the references",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Print debugging (trace level 1) information",
        )
        parser.add_argument(
            "--trace",
            type=int,
            metavar="NUMBER",
            help="Activate more detailed tracing (if NUMBER > 1)",
        )
        parser.add_argument(
            "--debug-tex",
            action="store_true",
            help="Print TeX's output (can be voluminous)",
        )
        parser.add_argument(
            "--debug-sourcefiles",
            type=Path,
            metavar="DIRECTORY",
            help="Keep unpacked/munged source files in DIRECTORY",
        )
        parser.add_argument(
            "--debug-pdftotext",
            action="store_true",
            help="Print pdftotext output",
        )

        settings = parser.parse_args(argv[1:])

        # We want logging ASAP but before we can do that, we need to check
        # pipeline mode, since that might affect where the logging output should
        # go.

        if settings.pipeline is None:
            # All logs need to go to stderr, since in non-pipeline mode our
            # stdout may be parsed.

            session_id = None
            input_stream = sys.stdin
            log_stream = sys.stderr
        else:
            # Session ID is determined from name of the directory containing the
            # input file.

            input_path = Path(settings.pipeline)
            session_id = input_path.parent.name
            input_stream = input_path.open("rt")

            # Set up directories

            log_root_path = os.environ.get("ADS_ARXIVREFS_LOGROOT")
            if log_root_path is None:
                print(
                    "fatal error: in --pipeline mode, $ADS_ARXIVREFS_LOGROOT must be set"
                )
                sys.exit(1)

            log_path = Path(log_root_path) / session_id
            print(
                f"ads_ref_extract: launching in pipeline mode, session id `{session_id}`"
            )
            print(
                f"ads_ref_extract: logs to `{log_path / 'extractrefs.stderr'}`",
                flush=True,
            )
            log_path.mkdir(parents=True, exist_ok=True)
            log_stream = (log_path / "extractrefs.stderr").open("wt")

        # OK, now we can set up the logging framework:

        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s\t%(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        default_logger.addHandler(handler)

        if settings.trace:
            settings.debug = True
        elif settings.debug:
            settings.trace = 1
        else:
            settings.trace = 0

        if settings.debug:
            default_logger.setLevel(logging.DEBUG + 1 - settings.trace)
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

        # Now common options.

        inst = cls()
        inst.config = config
        inst.logger = default_logger
        inst.force = settings.force
        inst.no_harvest = settings.no_harvest or settings.no_pdf
        inst.no_pdf = settings.no_pdf
        inst.no_tex = settings.no_tex
        inst.skip_refs = settings.skip_refs
        inst.debug_tex = settings.debug_tex
        inst.debug_source_files_dir = settings.debug_sourcefiles
        inst.debug_pdftotext = settings.debug_pdftotext
        inst.input_stream = input_stream
        inst.log_stream = log_stream

        # Not currently configurable, but it could be.
        inst.pdf_helper = (
            Path(__file__).parent.parent / "classic" / "extract_one_pdf.pl"
        )

        # Pipeline configurables:

        if session_id is None:
            inst.output_stream = sys.stdout
        else:
            inst.output_stream = (log_path / "extractrefs.out").open("wt")

        # In pipeline mode, now is the time to preserve the input specification,
        # since the input filepath won't be available downstream. We use this
        # file for post-processing analytics.

        if session_id is not None:
            input_clone_path = log_path / "fulltextharvest.out"
            n_to_do = 0

            with open(settings.pipeline, "rt") as f_in, open(
                input_clone_path, "wt"
            ) as f_out:
                for line in f_in:
                    n_to_do += 1
                    print(line, end="", file=f_out)

            print(f"ads_ref_extract: number of items to process: {n_to_do}", flush=True)

        return inst

    def process(self, input_stream=None, output_stream=None):
        self.logger.info("using the new Python extractrefs")
        t0 = time.time()
        n_inputs = 0
        n_failures = 0

        if input_stream is None:
            input_stream = self.input_stream

        if output_stream is None:
            output_stream = self.output_stream

        for line in input_stream:
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

            if target_ref_path == "withdrawn":
                # No refs to extract, but not a failure either:
                print(preprint_path, file=output_stream)
            elif target_ref_path is None:
                print(preprint_path, file=output_stream)
                n_failures += 1
            else:
                print(preprint_path, target_ref_path, file=output_stream)

        elapsed = time.time() - t0

        self.logger.info(f"processed {n_inputs} items")
        if n_inputs:
            rate = elapsed / n_inputs
            self.logger.info(
                f"elapsed time {elapsed:.0f}; processing rate: {rate:.1f} seconds per item"
            )
        if n_failures:
            self.logger.info(f"{n_failures} items could not be processed")
        return 0

    # Structured logging functions. These are intended to produce logging output
    # that is informative and readable, but still amenable to machine-processing
    # so that we can easily gather good analytics about the overall performance
    # of the pipeline.

    _current_item = "???"
    _failure_reason = None

    def item_info(self, summary: str, **kwargs):
        """
        Log something informational about the processing of an item. Use this
        function for data that will feed into analytics extracted from
        production runs.

        Parameters
        ==========
        summary : str
            A readable summary of the event. This text should be invariant, so that
            logs can be automatically summarized to keep track of the rates of
            various kinds of events
        **kwargs
            All event-specific information should be passed as kwargs. These
            will be included in the log message in `key=val` format, but in a
            way that allows them to be stripped out automatically.
        """
        # Sort for stable output across invocations
        details = " ".join(f"{t[0]}={t[1]}" for t in sorted(kwargs.items()))
        self.logger.info(f"% {summary} @i {self._current_item} {details}")

    def item_give_up(self, reason: str):
        """
        Log that we are giving up on extracting references for this item. Unlike
        other related functions, this method only takes one argument, which
        should be a terse summary of the failure reason. This reason is emitted
        in the summary output for the item.

        If this function is called multiple times during the processing of an
        item, only the first call will count. This generally fits the flow for
        diagnosing why processing failed.
        """
        if self._failure_reason is not None:
            return

        self._failure_reason = reason
        self.item_info("giving up", reason=reason)

    def item_warn(self, summary: str, **kwargs):
        """
        Log a warning about the processing of an item. Use this for cases where
        the assumptions of the pipeline appear to mistaken or expected
        invariants have failed to hold; e.g., simple failure to process a TeX
        file doesn't count.
        """
        details = " ".join(f"{t[0]}={t[1]}" for t in sorted(kwargs.items()))
        self.logger.warning(f"% {summary} @w {self._current_item} {details}")

    def item_trace1(self, summary: str, **kwargs):
        """
        Log a level-1 trace event about the processing of an item. Use this for
        high-level information about what's happening in the pipeline that is
        *not* needed for analytics.
        """
        details = " ".join(f"{t[0]}={t[1]}" for t in sorted(kwargs.items()))
        self.logger.debug(f"% {summary} @t1 {self._current_item} {details}")

    def item_trace2(self, summary: str, **kwargs):
        """
        Log a level-2 trace event about the processing of an item. Like trace1,
        but less important.
        """
        details = " ".join(f"{t[0]}={t[1]}" for t in sorted(kwargs.items()))
        self.logger.log(
            logging.DEBUG - 1, f"% {summary} @t2 {self._current_item} {details}"
        )

    def item_exec(self, argv: List[Union[str, Path]], silent=False, timeout=None):
        """
        Execute a subprocess on behalf of an item processing step.

        The main purpose of this wrapper is to make sure that the subprocess's
        standard streams are redirected appropriately.
        """
        argv = [str(x) for x in argv]

        if silent:
            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
        else:
            stdout = self.log_stream.buffer
            stderr = subprocess.STDOUT

        self.item_trace2("executing subprocess", argv=argv)
        subprocess.run(
            argv,
            shell=False,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            check=True,
            timeout=timeout,
        )

    def process_one(self, preprint_path: str, bibcode: Optional[str]) -> Optional[str]:
        """
        Process a single preprint.

        Returns the path to the newly created reference file if the preprint was
        processed successfully, or "withdrawn" for a withdrawn item. Returns
        None if it couldn't be processed.
        """
        item_stem, item_ext = split_item_path(preprint_path)
        item_id = item_stem  # might tweak this later
        self._current_item = item_id
        self._failure_reason = None
        self.item_info("begin", pp_path=preprint_path, bibcode=bibcode)
        exception = False

        try:
            tr_path = self._process_one_inner(bibcode, item_stem, item_ext)
            if tr_path is None:
                outcome = "fail"
            else:
                outcome = "success"
                self._failure_reason = "N/A"
        except Exception as e:
            tr_path = None
            self.item_warn("unhandled exception", e=e, c=e.__class__.__name__)
            self.logger.warning("detailed traceback:", exc_info=sys.exc_info())
            outcome = "fail"
            exception = True
            self._failure_reason = "unhandled-exception"

        if self._failure_reason is None:
            self.item_warn("processing failed without a logged reason")
            self._failure_reason = "uncaptured"

        self.item_info(
            "end", outcome=outcome, exception=exception, failwhy=self._failure_reason
        )
        self._current_item = "???"
        self._failure_reason = None
        return tr_path

    def _process_one_inner(
        self, bibcode: Optional[str], item_stem: str, item_ext: str
    ) -> Optional[str]:
        # Check out the fulltext source

        ft_path = self.config.fulltext_base / f"{item_stem}.{item_ext}"

        if not ft_path.exists():
            self.item_warn("cannot find expected fulltext", ft_path=ft_path)
            self.item_give_up("missing-fulltext")
            return None

        if item_ext in ("tar.gz", "tar", "tex.gz", "tex", "gz"):
            is_pdf = False
        elif item_ext in ("pdf", "pdf.gz"):
            is_pdf = True
        else:
            self.item_warn("unexpected input extension", ext=item_ext)
            self.item_give_up("unexpected-extension")
            return None

        # Check out the target refs file

        tr_path = self.config.target_refs_base / f"{item_stem}.raw"

        if not tr_path.exists():
            self.item_trace1("need to create output target-ref file", tr_path=tr_path)
        elif tr_path.stat().st_mtime < ft_path.stat().st_mtime:
            self.item_trace1("output target-ref file needs updating", tr_path=tr_path)
        elif self.force:
            self.item_trace1(
                "forcing recreation of output target-ref file", tr_path=tr_path
            )
        else:
            self.item_trace1("output target-ref file is up-to-date", tr_path=tr_path)
            return str(tr_path)

        # TODO: this is where classic guesses the bibcode and subdate if needed.
        if bibcode is None:
            self.item_warn("TEMP bailing because no bibcode")
            self.item_give_up("bibcode-unimplemented")
            return None

        wrote_refs = False

        if not is_pdf and not self.no_tex:
            if self.debug_source_files_dir is None:
                workdir = None
            else:
                import shutil

                workdir = self.debug_source_files_dir / item_stem.replace("/", "_")
                shutil.rmtree(workdir, ignore_errors=True)
                workdir.mkdir()
                self.item_trace1("preserving source files", p=workdir)

            try:
                from . import tex

                outcome = tex.extract_references(
                    self, ft_path, tr_path, bibcode, workdir=workdir
                )

                if isinstance(outcome, int):
                    if outcome < 0:
                        self.item_info("TeX-based extraction failed")
                    else:
                        wrote_refs = True
                elif isinstance(outcome, str):
                    if outcome == "withdrawn":
                        # This is sort of a failure, but not one we can do
                        # anything about.
                        return "withdrawn"
                    else:
                        raise Exception(f"unexpected outcome string `{outcome}`")
                else:
                    raise NotImplementedError()
            except Exception as e:
                self.item_warn("TeX extraction raised", e=e, c=e.__class__.__name__)
                self.logger.warning("detailed traceback:", exc_info=sys.exc_info())

        if not wrote_refs:
            # For now (?), farm out to the classic Perl implementation to try to
            # extract refs from the PDF.
            self.item_info(
                "attempting Perl-based PDF reference extraction",
                is_pdf=is_pdf,
            )

            if is_pdf:
                pdf_path = ft_path
            else:
                pdf_path = self.config.fulltext_base / f"{item_stem}.pdf"

            if not pdf_path.exists():
                self.item_warn("cannot find expected PDF", pdf_path=pdf_path)
                self.item_give_up("missing-pdf")
                return None

            tr_path.parent.mkdir(parents=True, exist_ok=True)
            argv = [str(self.pdf_helper), str(pdf_path), str(tr_path), bibcode]

            try:
                self.item_exec(argv)
            except subprocess.CalledProcessError as e:
                self.item_warn("PDF extraction failed", argv=argv, e=e)
            except Exception as e:
                self.item_warn(
                    "unexpected failure when trying PDF extraction", argv=argv, e=e
                )

            if tr_path.exists():
                self.item_info("Perl-based extraction seems to have worked")
            else:
                self.item_info("Perl-based extraction didn't create its output")
                return None

        return tr_path


def entrypoint(argv=sys.argv):
    sys.exit(CompatExtractor.new_from_commandline(argv).process())


if __name__ == "__main__":
    entrypoint()
