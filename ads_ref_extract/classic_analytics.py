"""
Performance analytics on the logfiles produced by a "classic" Arxiv reference
extraction session.
"""

import difflib
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys

from .utils import split_item_path

__all__ = [
    "ClassicSessionAnalytics",
    "ClassicSessionReprocessor",
    "analyze_session",
    "compare_outcomes",
    "compare_refstrings",
    "compare_resolved",
]

default_logger = logging.getLogger(__name__)


def _target_refs_for_session(extractrefs_out_path, reconstruct_targets, config, logger):
    """
    Yields sequence of ``(item_stem, item_ext, target_refs_path)``, eg:

    ``("arXiv/1905/03871", "tex", "/a/ads/data/refout/sources/arXiv/1905/03871.raw")``
    """

    with open(extractrefs_out_path, "rt") as er:
        for line in er:
            bits = line.strip().split()
            if not bits:
                logger.warn(f"unexpected empty line in `{extractrefs_out_path}`")
                continue

            item_stem, item_ext = split_item_path(bits[0])

            if len(bits) < 2:
                p = None
            elif reconstruct_targets:
                p = config.target_refs_base / (item_stem + ".raw")
            else:
                p = Path(bits[1])

            yield item_stem, item_ext, p


class ClassicSessionAnalytics(object):
    """
    A simple class for holding some analytics measurements relating to a single
    Arxiv processing session.
    """

    session_id = None
    "The name of the Arxiv processing session (of the form YYYY-MM-DD)."

    n_items = None
    "The number of Arxiv items sent to the reference extractor in the session."

    n_new_items = None
    "The (best guess of) the number of new submissions sent to the reference extractor."

    n_source_items = None
    "The number of items with TeX source sent to the extractor."

    n_emitted_items = None
    "The number of items for which reference information was emitted."

    n_refstrings = None
    "The total number of reference-string items emitted in the whole session."

    n_good_refs = None
    """The total number of references that were resolved to bibcodes with
    confidence, in the whole session. This may be None if the analytics
    computation was configured to not check the "resolved" files."""

    n_guess_refs = None
    """The total number of references that were resolved to bibcode guesses, in
    the whole session. This may be None if the analytics computation was
    configured to not check the "resolved" files."""

    def __str__(self):
        return f"""Classic session {self.session_id}:
    n_items = {self.n_items}
    n_new_items = {self.n_new_items}
    n_source_items = {self.n_source_items}
    n_emitted_items = {self.n_emitted_items}
    n_refstrings = {self.n_refstrings}
    n_good_refs = {self.n_good_refs}
    n_guess_refs = {self.n_guess_refs}"""

    def csv_header(self):
        """
        Return an array of strings corresponding to a header row for the data
        columns that would be returned by :meth:`as_csv_row`.

        Nothing about this function is actually specific to the CSV tabular
        format.
        """
        h = [
            "session_id",
            "items",
            "new_items",
            "source_items",
            "emitted_items",
            "refstrings",
        ]

        if self.n_good_refs is not None:
            h += [
                "good_refs",
                "guess_refs",
            ]

        return h

    def as_csv_row(self):
        """
        Return an array of strings capturing the contents of this object in a
        tabular form.

        Nothing about this function is actually specific to the CSV tabular
        format.
        """
        r = [
            self.session_id,
            str(self.n_items),
            str(self.n_new_items),
            str(self.n_source_items),
            str(self.n_emitted_items),
            str(self.n_refstrings),
        ]

        if self.n_good_refs is not None:
            r += [
                str(self.n_good_refs),
                str(self.n_guess_refs),
            ]

        return r


def analyze_session(
    session_id,
    config,
    logger=default_logger,
    reconstruct_targets=False,
    check_resolved=True,
):
    """
    Parse log files of a single processing session.

    If `reconstruct_targets` is set to True, we'll reconstruct the paths of the
    "target ref" files that contain the reference text extracted from each
    submission. Otherwise, we'll use the path contained in the `extractrefs.out`
    file. You might want to use this option if the reference-extraction run was
    done in a Docker container where the logged paths aren't valid on the host
    system.

    If `check_resolved` is set to False, we won't look at the "resolved" files
    in which reference text has been translated to bibcodes. You might want to
    use this if the resolution step hasn't been performed for the processing
    session that you are analyzing.

    """
    log_dir = config.classic_session_log_path(session_id)

    # First: analyze items that were in the update

    n_items = 0
    n_new = 0
    n_source = 0

    short_sid = session_id.replace("-", "")
    fth_path = log_dir / "fulltextharvest.out"

    with open(fth_path, "rt") as fth:
        for line in fth:
            bits = line.strip().split()
            if not bits:
                logger.warn(f"unexpected empty line in `{fth_path}`")
                continue

            n_items += 1
            _item_stem, item_ext = split_item_path(bits[0])

            if item_ext in ("tar.gz", "tex.gz"):
                n_source += 1
            elif item_ext in ("pdf",):
                pass
            else:
                logger.warn(
                    f"unexpected Arxiv item source type `{item_ext}` in `{fth_path}`"
                )

            if len(bits) > 3 and bits[3] == short_sid:
                n_new += 1

    # Next: analyze results of that update

    tref_info = _target_refs_for_session(
        log_dir / "extractrefs.out", reconstruct_targets, config, logger
    )
    raw_paths = [t[2] for t in tref_info if t[2] is not None]
    n_emitted = len(raw_paths)

    # Next: analyze items that had refstrings extracted
    #
    # NOTE: if some of these items were later updated, there might be some
    # inconsistencies between what was encountered during this particular
    # processing session and the state files on disk. Not sure if we should try
    # to do anything about that.

    n_refstrings = 0
    n_good_refs = 0
    n_guess_refs = 0

    if not check_resolved:
        n_good_refs = n_guess_refs = None

    for raw_path in raw_paths:
        # "refstrings" were extracted.
        #
        # We have at least one case
        # (/proj/ads/references/sources/arXiv/2111/05148.raw) where this file is
        # not UTF-8, so let's avoid assuming that.

        try:
            with open(raw_path, "rb") as raw_refs:
                for line in raw_refs:
                    if line.startswith(b"%Z"):
                        break

                for line in raw_refs:
                    if line.strip():
                        n_refstrings += 1
        except FileNotFoundError:
            logger.warn(
                f"unexpected missing ref target file `{raw_path}` for Arxiv session `{session_id}`"
            )
            continue
        except Exception as e:
            logger.warn(
                f"exception parsing ref target file `{raw_path}` for Arxiv session `{session_id}`: {e} ({e.__class__.__name__})"
            )
            continue

        # Resolved

        if not check_resolved:
            continue

        resolved_path = str(raw_path).replace("sources/", "resolved/") + ".result"

        try:
            with open(resolved_path, "rb") as resolved_refs:
                resolved_refs.readline()  # skip bibcode/ID info

                for line in resolved_refs:
                    bits = line.strip().split()
                    if not bits:
                        logger.warn(f"unexpected empty line in `{resolved_path}`")
                        continue

                    if bits[0] == b"1":
                        n_good_refs += 1
                    elif bits[0] == b"5":
                        n_guess_refs += 1
        except FileNotFoundError:
            logger.warn(
                f"unexpected missing ref resolved file `{resolved_path}` for Arxiv session `{session_id}`"
            )
            continue
        except Exception as e:
            logger.warn(
                f"exception parsing ref resolved file `{resolved_path}` for Arxiv session `{session_id}`: {e} ({e.__class__.__name__})"
            )
            continue

    # All done

    info = ClassicSessionAnalytics()
    info.session_id = session_id
    info.n_items = n_items
    info.n_new_items = n_new
    info.n_source_items = n_source
    info.n_emitted_items = n_emitted
    info.n_refstrings = n_refstrings
    info.n_good_refs = n_good_refs
    info.n_guess_refs = n_guess_refs
    return info


def compare_outcomes(
    session_id, A_config, B_config, logger=default_logger, ignore_pdfonly: bool = False
):
    """
    Given two processing passes of a single Arxiv update session, generate
    textual output summarizing the differences between the outcomes of
    processing the input items.

    This function is a generator yielding lines of text, including newline
    characters. You will typically invoke it as::

        for line in compare_outcomes(session_id, A_config, B_config):
            print(line, end='')

    The ``session_id`` is a string session ID, something like ``"2021-11-07"``.

    The ``A_config`` and ``B_config`` variables are Config objects that give
    path information about the files produced by the two processing runs.
    """

    # Scan the `extractrefs.out` outputs to discover which items were processed
    # and had refstrings extracted.

    er1 = A_config.classic_session_log_path(session_id) / "extractrefs.out"
    er2 = B_config.classic_session_log_path(session_id) / "extractrefs.out"

    A_results = dict(
        (t[0], t[1:]) for t in _target_refs_for_session(er1, True, A_config, logger)
    )
    B_results = dict(
        (t[0], t[1:]) for t in _target_refs_for_session(er2, True, B_config, logger)
    )

    # Do the high-level comparison

    stems = set(A_results.keys())
    stems.update(B_results.keys())

    regressions = set()
    fixes = set()
    n_preserves = 0
    n_fails = 0
    n_ignored_pdfs = 0

    for stem in stems:
        A_ext, A_path = A_results.get(stem, ("missing", None))
        B_ext, B_path = B_results.get(stem, ("missing", None))

        if ignore_pdfonly and A_ext == "pdf" or B_ext == "pdf":
            n_ignored_pdfs += 1
            continue

        if A_path is None:
            if B_path is None:
                n_fails += 1
            else:
                fixes.add(stem)
        elif B_path is None:
            regressions.add(stem)
        else:
            n_preserves += 1

    # Emit

    if fixes:
        yield "Fixed:\n"
        for stem in sorted(fixes):
            yield f"    {stem}\n"
        yield "\n"

    if regressions:
        yield "Regressed:\n"
        for stem in sorted(regressions):
            yield f"    {stem}\n"
        yield "\n"

    yield f">>> {len(fixes)} fixed items\n"
    yield f">>> {len(regressions)} regressed items\n"
    yield f">>> {n_preserves} preserved successes\n"
    yield f">>> {n_fails} unfixed failures\n"

    if ignore_pdfonly:
        yield f">>> {n_ignored_pdfs} ignored PDF-only items\n"


def compare_refstrings(session_id, A_config, B_config, logger=default_logger):
    """
    Given two processing passes of a single Arxiv update session, generate
    textual output summarizing the differences between the reference strings
    extracted by the two passes.

    This function is a generator yielding lines of text, including newline
    characters. You will typically invoke it as::

        for line in compare_refstrings(session_id, A_config, B_config):
            print(line, end='')

    The resulting output loosely resembles a "unified diff".

    The ``session_id`` is a string session ID, something like ``"2021-11-07"``.

    The ``A_config`` and ``B_config`` variables are Config objects that give
    path information about the files produced by the two processing runs.
    """

    # Scan the `extractrefs.out` output to discover which items were processed
    # and had refstrings extracted.

    er1 = A_config.classic_session_log_path(session_id) / "extractrefs.out"
    er2 = B_config.classic_session_log_path(session_id) / "extractrefs.out"

    A_results = dict(
        (t[0], t[1:]) for t in _target_refs_for_session(er1, True, A_config, logger)
    )
    B_results = dict(
        (t[0], t[1:]) for t in _target_refs_for_session(er2, True, B_config, logger)
    )

    # For each item, read the two associated sets of refstring outputs, emit the
    # differences between the two, and accumulate statistics.

    stems = set(A_results.keys())
    stems.update(B_results.keys())
    stems = sorted(stems)

    n_items_same = 0
    n_items_diff = 0
    n_refstrings_plus = 0
    n_refstrings_minus = 0

    for stem in stems:
        A_ext, A_path = A_results.get(stem, ("missing", None))
        B_ext, B_path = B_results.get(stem, ("missing", None))

        if A_path is None:
            A_lines = []
            A_desc = "(missing)"
        else:
            with open(A_path, "rb") as f:
                A_lines = f.readlines()[2:]
            A_desc = str(len(A_lines))

        if B_path is None:
            B_lines = []
            B_desc = "(missing)"
        else:
            with open(B_path, "rb") as f:
                B_lines = f.readlines()[2:]
            B_desc = str(len(B_lines))

        diffout = list(difflib.diff_bytes(difflib.unified_diff, A_lines, B_lines, n=0))

        if not len(diffout) and A_ext == B_ext:
            n_items_same += 1
            continue

        n_items_diff += 1

        if A_ext == B_ext:
            ext = A_ext
        else:
            ext = f"{A_ext} => {B_ext}"

        yield f"~~~ {stem}({ext}): {A_desc} => {B_desc}\n"

        for line in diffout:
            if (
                line.startswith(b"---")
                or line.startswith(b"+++")
                or line.startswith(b"@@")
            ):
                continue

            if line.startswith(b"+"):
                n_refstrings_plus += 1

            if line.startswith(b"-"):
                n_refstrings_minus += 1

            yield line.decode("utf-8", "backslashreplace")

    # Emit some summary statistics

    yield "\n"
    yield f">>> {n_items_same} unchanged items\n"
    yield f">>> {n_items_diff} changed items\n"
    yield f">>> {n_refstrings_plus} new refstring lines\n"
    yield f">>> {n_refstrings_minus} removed refstring lines\n"
    yield f">>> {n_refstrings_plus - n_refstrings_minus} net delta refstring lines\n"


class ClassicSessionReprocessor(object):
    """
    Helper class for reprocessing previously-processed sessions using a
    Dockerized version of the "classic" extractor. In principle this could be
    one big function call with a million arguments, but there are a lot of
    options and it's ergonomically helpful to have a class that lets you set
    them all up gradually.
    """

    image_name = None
    "The name of the Docker image with the classic-style reference extractor."

    config = None
    "The data path configuration."

    logs_out_base = None
    "The base directory for output log files."

    custom_app_dir = None
    "A local directory with a custom copy of the app to be mounted into the container."

    impl_kind = "python"
    "Which extractor implementation to use."

    capture_stderr = False
    "Whether stderr output should be saved to `extractrefs.stderr`"

    debug = False
    "Whether the extractor should be run in debugging mode"

    def __init__(self, config=None, image_name=None, logs_out_base=None):
        if config is not None:
            self.config = config

        if image_name is not None:
            self.image_name = image_name

        if logs_out_base is not None:
            self.logs_out_base = Path(logs_out_base)

    def _validate(self):
        if self.image_name is None:
            raise Exception("must set `image_name` before reprocessing")
        if self.config is None:
            raise Exception("must set `config` before reprocessing")
        if str(self.config.target_refs_base).startswith("/proj/ads/"):
            raise Exception(
                f"refusing to reprocess into target ref basedir `{self.config.target_refs_base}`"
            )
        if self.logs_out_base is None:
            raise Exception("must set `logs_out_base` before reprocessing")

    def reprocess(self, session_id):
        """
        Reprocess the specified session by invoking a Dockerized reference
        extractor.
        """

        self._validate()

        argv = [
            "docker",
            "run",
            "--rm",
            "-i",
            "--name",
            f"arxiv_refextract_repro_{session_id}",
            "-v",
            f"{self.config.abstracts_config_base}:/proj/ads/abstracts/config:ro,Z",
            "-v",
            f"{self.config.abstracts_links_base}:/proj/ads/abstracts/links:ro,Z",
            "-v",
            f"{self.config.fulltext_base}:/virtual_abstracts/sources/ArXiv/fulltext:ro,Z",
            "-v",
            f"{self.config.target_refs_base}:/refs_out:rw,Z",
        ]

        if self.custom_app_dir is not None:
            argv += ["-v", f"{self.custom_app_dir}:/app:ro,Z"]

        argv += [
            "-e",
            "ADS_ABSTRACTS",
            "-e",
            "ADS_REFERENCES",
            self.image_name,
            f"--impl-{self.impl_kind}",
            "--pbase",
            "/virtual_abstracts/sources/ArXiv/fulltext",
            "--tbase",
            "/refs_out",
            "--force",
        ]

        if self.debug:
            argv += ["--debug"]

        # Setup: output log file

        year = session_id.split("-")[0]
        logs_out_dir = self.logs_out_base / year / session_id
        os.makedirs(logs_out_dir, exist_ok=True)
        out_log_path = logs_out_dir / "extractrefs.out"
        stderr_path = logs_out_dir / "extractrefs.stderr"

        if self.capture_stderr:
            print(f"inner stderr captured to `{stderr_path}`", file=sys.stderr)

        # Setup: input processing specification. Copy the source file to the output
        # directory to make it simple to do analytics on later.

        source_input_path = (
            self.config.classic_session_log_path(session_id) / "fulltextharvest.out"
        )
        input_path = logs_out_dir / "fulltextharvest.out"
        shutil.copyfile(source_input_path, input_path)

        # Setup: inner environment. (This is mostly paranoia to avoid writing to
        # the production filesystem.)

        env = dict(os.environ)
        env["ADS_ABSTRACTS"] = "/virtual_abstracts"
        env["ADS_REFERENCES"] = "/virtual_references"

        # Ready to go!

        with open(input_path, "rb") as f_in, open(out_log_path, "wb") as f_out:
            if self.capture_stderr:
                with open(stderr_path, "wb") as f_err:
                    subprocess.check_call(
                        argv,
                        shell=False,
                        close_fds=True,
                        stdin=f_in,
                        stdout=f_out,
                        stderr=f_err,
                        env=env,
                    )
            else:
                subprocess.check_call(
                    argv, shell=False, close_fds=True, stdin=f_in, stdout=f_out, env=env
                )


def _maybe_load_raw_file(path, logger):
    MAX_RS_LEN = 512
    refstrings = set()

    if path is None:
        return refstrings

    before_refs = True

    with open(path, "rb") as f:
        for line in f:
            if before_refs:
                if line.startswith(b"%Z"):
                    before_refs = False
            else:
                rs = line.strip().decode("utf-8", "replace")

                if len(rs) > MAX_RS_LEN:
                    logger.debug(
                        f'truncating reference string "{rs[:20]}..." in `{path}`'
                    )
                    rs = rs[:MAX_RS_LEN]

                refstrings.add(rs)

    return refstrings


class ResolveComparison(object):
    """
    A simple class for holding some analytics measurements relating to two
    different reference extractions of a single Arxiv submission.

    We do this in a differential manner because these data consider not just
    "refstring" extraction but the resolution of those refstrings into actual
    ADS bibcodes. Such resolution is expensive, so we save a lot of resources if
    we only look at the difference between two sets of refstrings rather than
    computing all resolutions for both sets separately.
    """

    stem = None
    A_ext = None
    B_ext = None
    score_delta = None
    lost_resolutions = None

    def __init__(self):
        self.lost_resolutions = set()

    def __str__(self):
        return f"""Resolve comparison {self.stem}:
    exts = {self.A_ext}, {self.B_ext}
    score_delta = {self.score_delta}
    #lost = {len(self.lost_resolutions)}"""


def compare_resolved(
    session_id, A_config, B_config, rcache, logger=default_logger, **kwargs
):
    """
    Compare the results of reference extraction and resolution for two different
    passes over the same Arxiv session.

    This function returns a dictionary of results mapping Arxiv item "stems" (a
    string of the form ``"arXiv/2111/00061"``) to ResolveComparison objects.

    This function will resolve the reference strings to bibcodes using the ADS
    reference resolution microservice. It interfaces with this microservice via
    the ``rcache`` argument, which should be a ResolverCache instance. The
    resolver cache batches resultion requests and, yes, caches their results. It
    takes about 1--1.5 seconds to resolve a reference, so the resolution process
    can be slow.

    """
    # First, figure out which items in the two sessions were resolved.

    er1 = A_config.classic_session_log_path(session_id) / "extractrefs.out"
    er2 = B_config.classic_session_log_path(session_id) / "extractrefs.out"

    A_results = dict(
        (t[0], t[1:]) for t in _target_refs_for_session(er1, True, A_config, logger)
    )
    B_results = dict(
        (t[0], t[1:]) for t in _target_refs_for_session(er2, True, B_config, logger)
    )

    stems = set(A_results.keys())
    stems.update(B_results.keys())

    # Now figure out the diffs for each item and build up a list of reference
    # strings to resolve. By only looking at changed items, w decrease the
    # number of resolutions we need to perform by a factor of ~4.
    #
    # We batch up all of the references to resolve in order to make optimal use
    # of the resolver microservice API.

    A_uniques = {}
    B_uniques = {}
    to_resolve = set()
    results = {}

    for stem in stems:
        A_ext, A_path = A_results.get(stem, (None, None))
        B_ext, B_path = B_results.get(stem, (None, None))

        A_refstrings = _maybe_load_raw_file(A_path, logger)
        B_refstrings = _maybe_load_raw_file(B_path, logger)

        A_uniques[stem] = A_refstrings - B_refstrings
        B_uniques[stem] = B_refstrings - A_refstrings
        to_resolve.update(B_refstrings ^ A_refstrings)

        info = ResolveComparison()
        info.stem = stem
        info.A_ext = A_ext
        info.B_ext = B_ext

        results[stem] = info

    # Resolve all the things!

    resolved = rcache.resolve(to_resolve, **kwargs)

    # Postprocess analytics

    for stem in stems:
        info = results[stem]
        A_score = 0
        B_score = 0
        B_bibcodes = set()

        for rs in B_uniques[stem]:
            ri = resolved[rs]
            B_score += ri.score

            if ri.score > 0.5:
                B_bibcodes.add(ri.bibcode)

        for rs in A_uniques[stem]:
            ri = resolved[rs]
            A_score += ri.score

            if ri.score > 0.5 and ri.bibcode not in B_bibcodes:
                # Oh no, did we lose a good reference???
                info.lost_resolutions.add(rs)

        info.score_delta = B_score - A_score

    return results
