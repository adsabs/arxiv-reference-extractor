"""
Reference extraction from TeX source.

External tools used:

- pdflatex (and entire TeX stack, of course)
- pdftotext
- tar (for unpacking sources)
- zcat (for unpacking sources)

"""

import argparse
import binaryornot.check
import chardet
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Generator, List, Optional, Tuple, Union

from .compat import CompatExtractor

__all__ = ["extract_references"]


def extract_references(
    session: CompatExtractor,
    ft_path: Path,
    tr_path: Path,
    bibcode: str,
    workdir: Optional[Path] = None,
) -> Union[int, str]:
    """
    Extract references from an Arxiv TeX source package.

    Parameters
    ----------
    session : CompatExtractor
        The extraction session object
    ft_path : Path
        The absolute path of the fulltext source
    tr_path : Path
        The absolute path of the target output references file
    bibcode : str
        The bibcode associated with the ArXiv submission
    workdir : optional Path
        If provided, do the extraction and processing in the specified
        directory. Otherwise, do it in a temporary directory that is deleted at
        the end of processing.

    Returns
    -------
    If a nonnegative integer, the number of references extracted. This indicates
    successful extraction. If the ``skip_refs`` session setting is false, the
    reference strings will have been written into ``tr_path``. If the return
    value is a negative integer, extraction failed. If it is the string
    ``"withdrawn"``, the item was withdrawn and so there are no references to
    extract.

    Notes
    -----
    This function will change its working directory, so the input paths must be
    absolute.
    """
    orig_dir = os.getcwd()

    try:
        if workdir is not None:
            os.chdir(workdir)
            return _extract_inner(session, ft_path, tr_path, bibcode)
        else:
            with TemporaryDirectory() as tmpdir:
                os.chdir(tmpdir)
                return _extract_inner(session, ft_path, tr_path, bibcode)
    finally:
        os.chdir(orig_dir)


def _extract_inner(
    session: CompatExtractor,
    ft_path: Path,
    tr_path: Path,
    bibcode: str,
    until: Optional[str] = None,
) -> Union[int, str]:
    """
    The main extraction implementation, called with the CWD set to a new
    temporary directory.
    """

    # Unpack input.

    input_base = ft_path.name.lower()

    if input_base.endswith(".tar.gz") or input_base.endswith(".tgz"):
        # Unpack straight from the fulltext
        session.item_exec(["tar", "xzf", ft_path])
    elif input_base.endswith(".tar"):
        # Ditto
        session.item_exec(["tar", "xf", ft_path])
    elif input_base.endswith(".gz"):
        # Assume that other .gz files are directly compressed TeX. Ideally we wouldn't
        # rely on the shell to do the redirection here.
        session.item_trace2("guessing that fulltext is compressed TeX")
        outfn = input_base.replace(".gz", "")
        session.item_exec(["bash", "-c", f"zcat {ft_path} >{outfn}"])
    elif input_base.endswith(".tex"):
        # Just TeX
        shutil.copy(ft_path, input_base)
    else:
        # Assume that it's plain TeX with a weird/missing extension
        session.item_trace2("guessing that fulltext is funny-named straight TeX")
        outfn = input_base + ".tex"
        shutil.copy(ft_path, outfn)

    if until == "unpack":
        session.item_give_up("stop-at-unpack")
        return "earlyexit"  # NB, this should never escape to `extract_references()`

    # NOTE: classic used to use the submission date to determine which TeX stack
    # to use.

    # Probe the files to look for TeX sources and guess what the "main" TeX file
    # is. We can't know for sure until we actually try to compile, though.
    sources = TexSources.scan_cwd(session)

    # Munge the TeX sources to help us find references. Note that at this point
    # we still don't know what the main source file is!
    if sources.munge_refs(session):
        return "withdrawn"  # This indicates that this item was withdrawn

    if until == "munge":
        session.item_give_up("stop-at-munge")
        return "earlyexit"

    # Try compiling and seeing if we can pull out the refs
    dump_text = (until == "pdftotext") or session.debug_pdftotext
    refs = sources.extract_refs(session, dump_text=dump_text)
    if until == "extract" or until == "pdftotext":
        if not refs:
            session.item_info("extract-only mode: no references extracted")
        else:
            session.item_info("extract-only mode: got some references", n=len(refs))
            for ref in refs:
                session.item_info("     ref:", r=ref)

        session.item_give_up("stop-at-extract")
        return "earlyexit"

    # TODO(?): "see if changing the source .tex to include PDF files helps"
    # This changed .eps includes to .pdf and converted the corresponding files,
    # then recompiled.

    if not refs:
        # If we're here, something inside extract_refs() should have called item_give_up()
        session.item_info("unable to extract references from TeX source")
        return -1

    session.item_info("success getting refs from TeX", n=len(refs))

    if session.skip_refs:
        session.item_trace2("skipping writing references")
        session.item_give_up("skip-refs")
        return len(refs)

    tr_path.parent.mkdir(parents=True, exist_ok=True)

    with tr_path.open("wt", encoding="utf-8") as f:
        print(f"%R {bibcode}", file=f)
        print("%Z", file=f)
        for ref in refs:
            print(ref, file=f)

    return len(refs)


def _file_lines(
    p: Path, session: CompatExtractor
) -> Tuple[Optional[str], Optional[List[str]]]:
    """
    Read lines from a text file, guessing its encoding.

    Returns either a list of strings, representing the file line content, or
    None if we couldn't parse this file as text. The strings do not include line
    endings.

    This API is gross because we're working around rough edges in `chardet`.
    I've found that its incremental detection sometimes fails when
    whole-file-at-once succeeds, so we need to have everything in memory.
    """

    with open(p, "rb") as f:
        data = f.read()

    try:
        enc = "utf-8"
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        result = chardet.detect(data)
        enc = result.get("encoding")
        session.item_info("guessed text file encoding", p=p, **result)

        if enc is None or result.get("confidence") < 0.5:
            return None, None

        text = data.decode(enc)

    return enc, text.splitlines()


_START_REFS_REGEX = re.compile(
    r"\\begin\s*\{(chapthebibliography|thebibliography|references)\}", re.IGNORECASE
)
_HEX_CHARS = "0123456789abcdefABCDEF"
_FORCED_NEWLINE_REGEX = re.compile(r"\\bibitem|\\reference|\\end\{", re.IGNORECASE)
_END_REFS_REGEX = re.compile(
    r"^\s*\\end\s*\{(chapthebibliography|thebibliography|references)\}", re.IGNORECASE
)
_EMPH_REGEXES = [
    re.compile(r"\{\\em (.*?)\}"),
    re.compile(r"\{\\it (.*?)\}"),
    re.compile(r"\\emph\{(.*?)\}"),
    re.compile(r"\\textit\{(.*?)\}"),
]


def _split_on_delimited_prefix(text: str, open: str, close: str) -> Tuple[str, str]:
    """
    `open` and `close` are paired delimiters such as "{" and "}"

    `text` may start with whitespace and then the opening delimiter. If so, we
    trace through until we find the balanced closing delimiter, and split the
    text at that point. If we encounter a non-space, non-opener before finding
    either of those, the "left" split text is empty.
    """

    depth = 0

    for (idx, cur_char) in enumerate(text):
        if depth:
            if cur_char == open:
                depth += 1
            elif cur_char == close:
                depth -= 1

            if not depth:
                return text[: idx + 1], text[idx + 1 :]
            continue

        if cur_char.isspace():
            pass
        elif cur_char == open:
            depth += 1
        else:
            return ("", text)


_FIND_REF_REGEX = re.compile(r"<r>(.*?)<\s*/r\s*>", re.MULTILINE | re.DOTALL)


def _extract_references(text_path: Path, dump_stream=None) -> List[str]:
    with text_path.open("rt", encoding="utf8") as f:
        text = f.read()

    if dump_stream is not None:
        print(f"====== text extracted to {text_path} ======", file=dump_stream)
        print(text, file=dump_stream)
        print("====== end ======", file=dump_stream)

    refs = []

    while True:
        m = _FIND_REF_REGEX.search(text)
        if m is None:
            break

        refs.append(_postprocess_one_ref(m[1]))
        text = text[m.end() :]

    return refs


def _postprocess_one_ref(ref: str) -> str:
    # Hyphenations (which we try to prevent while munging)
    ref = ref.replace("-\n", "")

    # TODO: "split eprints"

    # Collapse and normalize whitespace
    ref = " ".join(ref.split())

    ref = ref.strip()
    return ref


_REF_EXTRA_OPENING = r"""
\newpage
\providecommand{\onecolumn}{\relax}
\onecolumn
\section*{}
\sloppy
\raggedright
\hyphenpenalty=10000
\exhyphenpenalty=10000
\def\UrlBreaks{}
\def\UrlBigBreaks{}
\def\UrlNoBreaks{\do\:\do\-}
{\textless}r{\textgreater}"""
_REF_EXTRA_CLOSING = r"{\textless}/r{\textgreater}"
_OUTPUT_WRITTEN_REGEX = re.compile(rb"Output written on (.*) \(")


class TexSourceItem(object):
    path: Path
    "The path to the source file"

    score: int
    """A numerical score indicating our confidence that the file is the main TeX
    file. Higher is more confident."""

    bibitem: str
    "The TeX command used to declare bibliography items in this file; possibly custom."

    title: str
    "A guess of the document title derived from this file."

    fmt: str
    'The TeX format to use, derived from this file: maybe "latex"'

    ignore: bool
    "A flag to definitely ignore this input file."

    def __init__(self, path: Path):
        self.path = path
        self.score = 0
        self.bibitem = ""
        self.title = ""
        self.fmt = ""
        self.ignore = False

    def munge_refs(self, session: CompatExtractor):
        if self.ignore:
            session.item_trace2("skipping munging of ignored file", p=self.path)
            return 0

        # Perl has \Q/\E to make sure that if self.bibitem contains regex
        # special characters, they're treated as literals. Basic Python regexes
        # don't seem to support that.
        start_item = re.compile(
            rf"^\s*\\(bibitem|reference|rn|rf|rfprep|item|{self.bibitem})\b(.*)",
            re.IGNORECASE,
        )

        enc, in_lines = _file_lines(self.path, session)
        assert in_lines is not None, "encoding detection failed second time??"
        in_lines = iter(in_lines)  # ensure that our loops progress through the list

        with NamedTemporaryFile(
            mode="wt", encoding=enc, dir=self.path.parent, delete=False
        ) as f_out:
            s = str(self.path).lower()

            if s.endswith(".bib") or s.endswith(".bbl"):
                # Munge these files, but don't try to find the references
                # section
                pass
            else:
                for line in in_lines:
                    print(line, file=f_out)
                    if _START_REFS_REGEX.search(line) is not None:
                        break

            line_in_progress = ""
            tag = None
            cur_ref = ""
            ref_type = ""
            n_tagged = 0

            for line in in_lines:
                # High-level processing of whitespace and comments. Collapsing
                # comments can make a big difference for \bibitem commands that
                # are sometimes split across lines in "exciting" ways. On the
                # other hand, other sources use %'s aggressively such that if we
                # don't force some line splits, our line-based parser will fail.

                line = line_in_progress + line
                line_in_progress = ""

                try:
                    cidx = line.index("%")
                except ValueError:
                    pass
                else:
                    # When is a percent not a comment?
                    # - When it's \%
                    # - Inside a \href where it's acting as percent-encoding
                    # - Surely other cases to be added, as well
                    if (
                        len(line) > cidx + 2
                        and line[cidx + 1] in _HEX_CHARS
                        and line[cidx + 2] in _HEX_CHARS
                    ):
                        pass
                    elif cidx == 0:
                        # Note: we need to remove all comments so that our </r>s
                        # make it into the output
                        continue
                    elif line[cidx - 1] != "\\":
                        line_in_progress = line[:cidx] + " "
                        continue

                m = _FORCED_NEWLINE_REGEX.search(line[1:])
                if m is not None:
                    line_in_progress = line[m.start() + 1 :]
                    line = line[: m.start() + 1]

                if _END_REFS_REGEX.search(line) is not None:
                    if cur_ref:
                        # Need to emit this before emitting the \end{references} line:
                        self.tag_ref(tag, cur_ref, ref_type, f_out)
                        n_tagged += 1
                        cur_ref = ""

                    print(line, file=f_out)
                    break

                # TODO: extractrefs.pl does this; not sure about the value:
                #   s/\b(\w+\s*)--(\s*\w+)\b/$1-$2/g;

                m = start_item.match(line)
                if m is not None:
                    # Start a new item
                    if tag is None:
                        tag = m[1]

                        if tag in ("bibitem", self.bibitem):
                            ref_type = "bibitem"
                        elif tag in ("reference", "ref"):
                            ref_type = "reference"

                    if cur_ref:
                        self.tag_ref(tag, cur_ref, ref_type, f_out)
                        n_tagged += 1

                    cur_ref = m[2]
                elif tag is not None:
                    # In the middle of an item
                    cur_ref += line
                else:
                    # Still looking for the actual bib items
                    print(line, file=f_out)

            if cur_ref:
                self.tag_ref(tag, cur_ref, ref_type, f_out)
                n_tagged += 1

            if line_in_progress:
                print(line_in_progress, file=f_out)

            for line in in_lines:
                print(line, file=f_out)

        # All done!
        session.item_trace1("finished munging a file", n_tagged=n_tagged, p=self.path)
        os.rename(f_out.name, self.path)
        return n_tagged

    def tag_ref(self, tag: str, text: str, ref_type: str, f_out):
        """
        Format of the input should be: `\{tag}{text}`, where `text` is
        potentially multiple lines.

        If `ref_type` is "bibitem", we expect:

        - \bibitem{latextag} Ref text ...
        - \bibitem[alt]{latextag} Ref text ...

        If it is "reference", we expect:

        - \reference{bibcode} Ref text ... (old emulateapj)
        - \reference Ref text ...

        Otherwise we expect `\{tag} Ref text ...` (e.g., consistent with the
        second \reference option).
        """

        # The munging applied here has a non-trivial impact on the success of
        # the reference resolution stage:
        for regex in _EMPH_REGEXES:
            text = regex.sub(r'"\1"', text)

        if ref_type == "bibitem":
            left, text = _split_on_delimited_prefix(text, "[", "]")
            tag += left
            left, text = _split_on_delimited_prefix(text, "{", "}")
            tag += left
        elif ref_type == "reference":
            left, text = _split_on_delimited_prefix(text, "{", "}")
            tag += left

        print("\\" + tag, _REF_EXTRA_OPENING, text, _REF_EXTRA_CLOSING, file=f_out)

    def extract_refs_as_main_file(
        self, session: CompatExtractor, dump_text=False
    ) -> List[str]:
        session.item_trace1("trying a TeX build", main_file=self.path)

        if self.ignore:
            session.item_trace2("actually, ignoring due to flag")
            return []

        command = [
            str(session.config.tex_bin_dir / "pdflatex"),
            "-interaction=nonstopmode",
            str(self.path),
        ]

        try:
            session.item_exec(
                command,
                silent=not session.debug_tex,
                timeout=100,
            )
        except subprocess.TimeoutExpired:
            session.item_trace1("TeX timed out")
        except subprocess.CalledProcessError as e:
            session.item_trace2("TeX failed", e=e)
        except Exception as e:
            session.item_warn("unexpected failure when trying to TeX", e=e)

        base = str(self.path).rsplit(".", 1)[0]
        pdf_path = Path(base + ".pdf")
        log_path = Path(base + ".log")

        # See if the logfile gives us a better output name
        try:
            f = log_path.open("rb")
        except FileNotFoundError:
            pass
        else:
            try:
                with f:
                    for line in f:
                        m = _OUTPUT_WRITTEN_REGEX.match(line)
                        if m is not None:
                            pdf_path = Path(m[1].decode(errors="surrogateescape"))
            except Exception as e:
                # This will happen if the log
                session.item_trace2(
                    "couldn't scan logfile", e=e, c=e.__class__.__name__
                )

        try:
            if pdf_path.stat().st_size == 0:
                session.item_trace2("TeX was OK but output has 0 size", p=pdf_path)

                try:
                    pdf_path.unlink()
                except Exception as e:
                    session.item_warn(
                        "error unlinking empty TeX output file", p=pdf_path, e=e
                    )
                return []
        except FileNotFoundError:
            # This isn't a warning -- this will happen often in regular operations
            session.item_trace1(
                "expected TeX output file not found", pmain=self.path, ppdf=pdf_path
            )
            return []
        except Exception as e:
            session.item_warn(
                "error stat'ing TeX output file", pmain=self.path, ppdf=pdf_path, e=e
            )
            return []

        # Looks like we have a PDF!

        text_path = Path(str(pdf_path) + ".txt")
        session.item_info(
            "got a munged-TeX PDF", pmain=self.path, ppdf=pdf_path, ptext=text_path
        )
        session.item_exec(["pdftotext", "-raw", "-enc", "UTF-8", pdf_path, text_path])
        return _extract_references(
            text_path, dump_stream=session.log_stream if dump_text else None
        )


_BASENAME_SCORE_DELTAS = {
    "mn2eguide": -100,
    "mn2esample": -100,
    "mnras_guide": -100,
    "aa": -100,
    "new_feat": -50,
    "rnaas": -5,
    "mnras_template": -2,  # "some people put their paper in this file!"
}

_LATEX_DOCCLASS_REGEXES = [
    re.compile(r"^\s+\\begin\s\{document\}"),
    re.compile(r"^\s*[^%$].*?\\begin\s*\{document\}"),
    re.compile(r"^\s*\\documentclass\b"),
    re.compile(r"^\s*\\documentstyle\b"),
]

_TEX_MAIN_FILE_REGEXES = [
    re.compile(r"^\\title\{", re.IGNORECASE),
    re.compile(r"^\s*\\begin\s*\{abstract\}\b", re.IGNORECASE),
    re.compile(r"^\s*\\section\s*\{introduction\}\b", re.IGNORECASE),
    re.compile(
        r"^\s*\\begin\s*{(chapthebibliography|thebibliography|references)\}",
        re.IGNORECASE,
    ),
]


def _match_any(text: str, regex_list: List[re.Pattern]) -> Optional[re.Match]:
    for regex in regex_list:
        m = regex.match(text)
        if m is not None:
            return m

    return None


def _probe_one_source(
    filepath: Path, non_main_files: set, session: CompatExtractor
) -> Optional[TexSourceItem]:
    s = str(filepath.name).lower()
    item = TexSourceItem(filepath)

    if "psfig" in s:
        return None

    # Text-like files that definitely aren't TeX sources:
    if s.endswith(".eps"):
        return None

    if binaryornot.check.is_binary(str(filepath)):
        session.item_trace2(
            "skipping potential TeX source: detected as binary", p=filepath
        )
        return None

    if (
        s.endswith(".tex")
        or s.endswith(".ltx")
        or s.endswith(".latex")
        or s.endswith(".revtex")
    ):
        item.score += 1
    elif s.endswith(".bib") or s.endswith(".bbl"):
        pass
    elif s.endswith(".txt") or "." not in s:
        pass
    else:
        return None

    basename = s.rsplit(".", 1)[0]
    item.score += _BASENAME_SCORE_DELTAS.get(basename, 0)
    session.item_trace2("scanning potential TeX source", p=filepath, score=item.score)

    try:
        _enc, lines = _file_lines(filepath, session)
        if lines is None:
            session.item_warn(
                "failed to interpret potential TeX source as text", p=filepath
            )
            return None

        for line in lines:
            if "%auto-ignore" in line:
                item.ignore = True
                break

            if _match_any(line, _LATEX_DOCCLASS_REGEXES) is not None:
                item.fmt = "latex"
                item.score += 1

            if _match_any(line, _TEX_MAIN_FILE_REGEXES) is not None:
                item.score += 1
                continue

            m = re.match(r"^\s*\\shorttitle\s*\{(.*)\}", line, re.IGNORECASE)
            if m is not None:
                item.title = m[1]
                item.score += 1
                continue

            m = re.match(
                r"^\s*\\newcommand\s*\{\\([^\}]+)\}.*?\{\\bibitem\b",
                line,
                re.IGNORECASE,
            )
            if m is not None:
                item.bibitem = m[1]
                continue

            m = re.match(r"^\s*\\def\{?\\(.+?)\{\\bibitem\b", line, re.IGNORECASE)
            if m is not None:
                item.bibitem = m[1]
                continue

            m = re.match(r"^\s*\\input\{\s*(\S*?)\s*\}", line)
            if m is not None:
                non_main_files.add(m[1])
                continue

            m = re.match(r"^\s*\\input\s+(\S*?)", line)
            if m is not None:
                non_main_files.add(m[1])
                continue
    except Exception as e:
        session.item_warn("failed to scan potential TeX source", p=filepath, e=e)
        return None

    return item


def _find_files_cwd() -> Generator[Path, None, None]:
    todo = [Path.cwd()]

    while len(todo):
        dir = todo.pop()

        for item in dir.iterdir():
            if item.is_dir():
                todo.append(item)
            elif item.is_file():
                yield item


class TexSources(object):
    items: List[TexSourceItem]

    @classmethod
    def scan_cwd(cls, session: CompatExtractor) -> "TexSources":
        items: List[TexSourceItem] = []
        non_main_files = set()

        # Get the base list

        for filepath in _find_files_cwd():
            item = _probe_one_source(filepath, non_main_files, session)
            if item is None:
                continue

            items.append(item)

        # Futz scores based on inclusions

        for item in items:
            s = str(item.path)

            if s in non_main_files:
                item.score = -2
            elif s.rsplit(".", 1)[0] in non_main_files:
                item.score = -1

        # Sort.
        items = sorted(items, key=lambda i: i.score, reverse=True)

        # Gather and apply defaulted meta-fields
        default_bibitem = None
        default_title = None

        for item in items:
            if default_bibitem is None and item.bibitem:
                default_bibitem = item.bibitem
            if default_title is None and item.title:
                default_title = item.title

        if default_bibitem is None:
            default_bibitem = "bibitem"

        for item in items:
            if not item.bibitem:
                item.bibitem = default_bibitem
            if not item.title:
                item.title = default_title

        # All done
        inst = cls()
        inst.items = items
        return inst

    def munge_refs(self, session: CompatExtractor):
        """
        Returns True if the submission was withdrawn, which is indicated by a
        single source file marked with %auto-ignore. This could be detected
        earlier but currently this is a convenient place for the check.
        """

        n_total = 0

        for item in self.items:
            n_total += item.munge_refs(session)

        if n_total == 0:
            if len(self.items) == 1 and self.items[0].ignore:
                # We don't classify this as a give-up because it's not a
                # failure; there's nothing to do.
                session.item_info("withdrawn")
                return True
            else:
                session.item_warn("didn't find anything to munge")

        return False

    def extract_refs(self, session: CompatExtractor, dump_text=False) -> List[str]:
        for item in self.items:
            refs = item.extract_refs_as_main_file(session, dump_text=dump_text)
            if refs:
                return refs

        session.item_give_up("no-main-file-worked")
        return []


# CLI helper support


def _get_quick_logger():
    import logging

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s\t%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    logger = logging.getLogger("tex-cli")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def _do_one(settings, until):
    from .config import Config

    session = CompatExtractor()
    session.config = Config.new_defaults()
    session.logger = _get_quick_logger()
    session.log_stream = sys.stderr
    session.output_stream = sys.stdout

    ft_path = Path(settings.fulltext).absolute()

    if settings.workdir is None:
        with TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            session.logger.info(f"CLI harness: working in tempdir `{tmpdir}`")
            _extract_inner(session, ft_path, None, "N/A", until=until)
    else:
        os.chdir(settings.workdir)
        _extract_inner(session, ft_path, None, "N/A", until=until)


def entrypoint():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="subcommand")

    def declare_generic(name):
        p = commands.add_parser(name)
        p.add_argument(
            "fulltext", metavar="PATH", help="The path to the Arxiv fulltext file"
        )
        p.add_argument(
            "workdir", nargs="?", metavar="PATH", help="The path to extract to"
        )

    declare_generic("extract")
    declare_generic("munge")
    declare_generic("pdftotext")
    declare_generic("unpack")

    settings = parser.parse_args()
    if settings.subcommand is None:
        raise Exception("use a subcommand: extract|munge|unpack")

    if settings.subcommand == "extract":
        _do_one(settings, "extract")
    elif settings.subcommand == "munge":
        _do_one(settings, "munge")
    elif settings.subcommand == "pdftotext":
        _do_one(settings, "pdftotext")
    elif settings.subcommand == "unpack":
        _do_one(settings, "unpack")
    else:
        raise Exception(
            f"unknown subcommand `{settings.subcommand}`; run without arguments for a list"
        )


if __name__ == "__main__":
    entrypoint()

# For historical interest: cutover dates for different versions of ArXiv TeX
# stack. "As per Thorsten's recipe of 2012-01-20" with updates. Cutover dates
# are such that `(subdate >= cutover) -> {use this stack}`.
#
# ???????? -> TeXLive 2020
# 20170209 -> TeXLive 2016
# 20111206 -> TeXLive 2011
# 20091231 -> TeXLive 2009
# 20061102 -> TeTeX 3
# 20040101 -> TeTeX 2, `texmf-2004`
# 20030101 -> TeTeX 2, `texmf-2003`
# 20020901 -> TeTeX 2, `texmf-2002`
# 0        -> TeTeX 2, `texmf`
