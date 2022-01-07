"""
Reference extraction from TeX source.

External tools used:

- pdflatex (and entire TeX stack, of course)
- pdftotext
- tar (for unpacking sources)
- zcat (for unpacking sources)

"""

import argparse
from logging import Logger
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Generator, List, Optional, Tuple

from .compat import CompatExtractor

__all__ = ["extract_references"]


def extract_references(
    session: CompatExtractor,
    ft_path: Path,
    tr_path: Path,
    bibcode: str,
) -> bool:
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

    Returns
    -------
    Whether references were successfully extracted.

    Notes
    -----
    This function will change its working directory, so the input paths must be absolute.
    """
    orig_dir = os.getcwd()

    try:
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
) -> bool:
    """
    The main extraction implementation, called with the CWD set to a new
    temporary directory.
    """

    # Unpack input.

    input_base = ft_path.name.lower()

    if input_base.endswith(".tar.gz") or input_base.endswith(".tgz"):
        # Unpack straight from the fulltext
        subprocess.check_call(["tar", "xzf", str(ft_path)], shell=False)
    elif input_base.endswith(".tar"):
        # Ditto
        subprocess.check_call(["tar", "xf", str(ft_path)], shell=False)
    elif input_base.endswith(".gz"):
        # Assume that other .gz files are directly compressed TeX. Ideally we wouldn't
        # rely on the shell to do the redirection here.
        session.item_trace2("guessing that fulltext is compressed TeX")
        outfn = input_base.replace(".gz", "")
        subprocess.check_call(f"zcat {ft_path} >{outfn}", shell=True)
    elif input_base.endswith(".tex"):
        # Just TeX
        shutil.copy(ft_path, input_base)
    else:
        # Assume that it's plain TeX with a weird/missing extension
        session.item_trace2("guessing that fulltext is funny-named straight TeX")
        outfn = input_base + ".tex"
        shutil.copy(ft_path, outfn)

    if until == "unpack":
        return False

    # NOTE: classic used to use the submission date to determine which TeX stack
    # to use.

    # Probe the files to look for TeX sources and guess what the "main" TeX file
    # is. We can't know for sure until we actually try to compile, though.
    sources = TexSources.scan_cwd(session)

    # Munge the TeX sources to help us find references. Note that at this point
    # we still don't know what the main source file is!
    sources.munge_refs(session)
    if until == "munge":
        return False

    # Try compiling and seeing if we can pull out the refs
    refs = sources.extract_refs(session)
    if until == "extract":
        if not refs:
            session.item_info("extract-only mode: no references extracted")
        else:
            session.item_info("extract-only mode: got some references", n=len(refs))
            for ref in refs:
                session.item_info("     ref:", r=ref)
        return False

    # TODO(?): "see if changing the source .tex to include PDF files helps"
    # This changed .eps includes to .pdf and converted the corresponding files,
    # then recompiled.

    if session.skip_refs:
        session.item_trace2("skipping writing references")
        return False

    tr_path.parent.mkdir(parents=True, exist_ok=True)

    with tr_path.open("wt", encoding="utf-8") as f:
        print(f"%R {bibcode}", file=f)
        print("%Z", file=f)
        for ref in refs:
            print(ref, file=f)

    return True


_START_REFS_REGEX = re.compile(
    r"\\begin\s*\{(chapthebibliography|thebibliography|references)\}", re.IGNORECASE
)
_END_REFS_REGEX = re.compile(
    r"^\s*\\end\s*\{(chapthebibliography|thebibliography|references)\}", re.IGNORECASE
)


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


def _extract_references(text_path: Path) -> List[str]:
    with text_path.open("rt", encoding="utf8") as f:
        text = f.read()

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


_REF_EXTRA_OPENING = r"\newpage\onecolumn\section*{}$<$r$>$\sloppy\raggedright"
_REF_EXTRA_CLOSING = r"$<$/r$>$"
_OUTPUT_WRITTEN_REGEX = re.compile(r"Output written on (.*) \(")


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
            return

        # Perl has \Q/\E to make sure that if self.bibitem contains regex
        # special characters, they're treated as literals. Basic Python regexes
        # don't seem to support that.
        start_item = re.compile(
            rf"^\s*\\(bibitem|reference|rn|rf|rfprep|item|{self.bibitem})\b(.*)",
            re.IGNORECASE,
        )

        with open(self.path, "rt") as f_in, NamedTemporaryFile(
            mode="wt", dir=self.path.parent, delete=False
        ) as f_out:
            s = str(self.path).lower()

            if s.endswith(".bib") or s.endswith(".bbl"):
                # Munge these files, but don't try to find the references
                # section
                pass
            else:
                for line in f_in:
                    print(line, end="", file=f_out)
                    if _START_REFS_REGEX.match(line) is not None:
                        break

            tag = None
            cur_ref = ""
            ref_type = ""
            n_tagged = 0

            for line in f_in:
                s = line.strip()
                if not s or s.startswith("%"):
                    continue

                # TODO: implement {\em ...} munging here.

                if _END_REFS_REGEX.match(line) is not None:
                    if cur_ref:
                        # Need to emit this before emitting the \end{references} line:
                        self.tag_ref(tag, cur_ref, ref_type, f_out)
                        n_tagged += 1
                        cur_ref = ""

                    print(line, end="", file=f_out)
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
                    print(line, end="", file=f_out)

            if cur_ref:
                self.tag_ref(tag, cur_ref, ref_type, f_out)
                n_tagged += 1

            for line in f_in:
                print(line, end="", file=f_out)

        # All done!
        session.item_trace1("finished munging a file", n_tagged=n_tagged, p=self.path)
        os.rename(f_out.name, self.path)

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
        if ref_type == "bibitem":
            left, text = _split_on_delimited_prefix(text, "[", "]")
            tag += left
            left, text = _split_on_delimited_prefix(text, "{", "}")
            tag += left
        elif ref_type == "reference":
            left, text = _split_on_delimited_prefix(text, "{", "}")
            tag += left

        print("\\" + tag, _REF_EXTRA_OPENING, text, _REF_EXTRA_CLOSING, file=f_out)

    def extract_refs_as_main_file(self, session: CompatExtractor) -> List[str]:
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
            subprocess.run(
                command,
                shell=False,
                stdout=subprocess.DEVNULL,  # temporary??
                stderr=subprocess.DEVNULL,  # temporary??
                timeout=100,
                check=True,
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
            f = log_path.open("rt")
        except FileNotFoundError:
            pass
        else:
            with f:
                for line in f:
                    m = _OUTPUT_WRITTEN_REGEX.match(line)
                    if m is not None:
                        pdf_path = Path(m[1])

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
            session.item_warn(
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
        subprocess.check_call(
            ["pdftotext", "-raw", "-enc", "UTF-8", str(pdf_path), str(text_path)],
            shell=False,
        )
        return _extract_references(text_path)


_BASENAME_SCORE_DELTAS = {
    "mn2eguide": -100,
    "mn2esample": -100,
    "mnras_guide": -100,
    "aa": -100,
    "new_feat": -50,
    "rnaas": -5,
    "mnras_template": -2,  # "some people put their paper in this file!
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
    s = str(filepath).lower()
    item = TexSourceItem(filepath)

    if "psfig" in s:
        return None

    # not in classic: skip files that we definitely don't want, to reduce useless output
    if (
        s.endswith(".pdf")
        or s.endswith(".jpg")
        or s.endswith(".jpeg")
        or s.endswith(".png")
        or s.endswith(".xml")
        or s.endswith(".psd")
        or s.endswith(".mp4")
    ):
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
    elif s.endswith(".txt") or not s.startswith("."):
        pass
    else:
        return None

    basename = s.rsplit(".", 1)[0]
    item.score += _BASENAME_SCORE_DELTAS.get(basename, 0)
    session.item_trace2("scanning potential TeX source", p=filepath)

    try:
        # TODO: guess encoding?
        with filepath.open("rt") as f:
            for line in f:
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
        for item in self.items:
            item.munge_refs(session)

    def extract_refs(self, session: CompatExtractor) -> List[str]:
        for item in self.items:
            refs = item.extract_refs_as_main_file(session)
            if refs:
                return refs

        session.item_trace1("couldn't extract refs for any input file :-(")
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

    ft_path = Path(settings.fulltext).absolute()

    if settings.workdir is None:
        with TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            session.logger.info(f"CLI harness: working in tempdir `{tmpdir}`")
            _extract_inner(session, "CLI", ft_path, None, "N/A", until=until)
    else:
        os.chdir(settings.workdir)
        _extract_inner(session, "CLI", ft_path, None, "N/A", until=until)


def entrypoint():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="subcommand")

    extract = commands.add_parser("extract")
    extract.add_argument(
        "fulltext", metavar="PATH", help="The path to the Arxiv fulltext file"
    )
    extract.add_argument(
        "workdir", nargs="?", metavar="PATH", help="The path to extract to"
    )

    munge = commands.add_parser("munge")
    munge.add_argument(
        "fulltext", metavar="PATH", help="The path to the Arxiv fulltext file"
    )
    munge.add_argument(
        "workdir", nargs="?", metavar="PATH", help="The path to extract to"
    )

    unpack = commands.add_parser("unpack")
    unpack.add_argument(
        "fulltext", metavar="PATH", help="The path to the Arxiv fulltext file"
    )
    unpack.add_argument(
        "workdir", nargs="?", metavar="PATH", help="The path to extract to"
    )

    settings = parser.parse_args()
    if settings.subcommand is None:
        raise Exception("use a subcommand: extract|munge|unpack")

    if settings.subcommand == "extract":
        _do_one(settings, "extract")
    elif settings.subcommand == "munge":
        _do_one(settings, "munge")
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
