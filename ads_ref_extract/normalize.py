# -*- coding: utf-8 -*-
#
# Derived from https://github.com/gorgitko/molminer : molminer/normalize.py
# In turn derived from https://github.com/mcs07/ChemDataExtractor : chemdataextractor/text/normalize.py
#
# MIT Licensed
#
# Reformatted with black

from abc import ABC, abstractmethod
import unicodedata

__all__ = ["refstring_normalizer"]

#: Control characters.
CONTROLS = {
    "\u0001",
    "\u0002",
    "\u0003",
    "\u0004",
    "\u0005",
    "\u0006",
    "\u0007",
    "\u0008",
}

#: Hyphen and dash characters.
HYPHENS = {
    "-",  # \u002d Hyphen-minus
    "‐",  # \u2010 Hyphen
    "‑",  # \u2011 Non-breaking hyphen
    "⁃",  # \u2043 Hyphen bullet
    "‒",  # \u2012 figure dash
    "–",  # \u2013 en dash
    "—",  # \u2014 em dash
    "―",  # \u2015 horizontal bar
}

#: Minus characters.
MINUSES = {
    "-",  # \u002d Hyphen-minus
    "−",  # \u2212 Minus
    "－",  # \uff0d Full-width Hyphen-minus
    "⁻",  # \u207b Superscript minus
}

#: Plus characters.
PLUSES = {
    "+",  # \u002b Plus
    "＋",  # \uff0b Full-width Plus
    "⁺",  # \u207a Superscript plus
}

#: Slash characters.
SLASHES = {
    "/",  # \u002f Solidus
    "⁄",  # \u2044 Fraction slash
    "∕",  # \u2215 Division slash
}

#: Tilde characters.
TILDES = {
    "~",  # \u007e Tilde
    "˜",  # \u02dc Small tilde
    "⁓",  # \u2053 Swung dash
    "∼",  # \u223c Tilde operator
    "∽",  # \u223d Reversed tilde
    "∿",  # \u223f Sine wave
    "〜",  # \u301c Wave dash
    "～",  # \uff5e Full-width tilde
}

#: Apostrophe characters.
APOSTROPHES = {
    "'",  # \u0027
    "’",  # \u2019
    "՚",  # \u055a
    "Ꞌ",  # \ua78b
    "ꞌ",  # \ua78c
    "＇",  # \uff07
}

#: Single quote characters.
SINGLE_QUOTES = {
    "'",  # \u0027
    "‘",  # \u2018
    "’",  # \u2019
    "‚",  # \u201a
    "‛",  # \u201b
}

#: Double quote characters.
DOUBLE_QUOTES = {
    '"',  # \u0022
    "“",  # \u201c
    "”",  # \u201d
    "„",  # \u201e
    "‟",  # \u201f
}

#: Accent characters.
ACCENTS = {
    "`",  # \u0060
    "´",  # \u00b4
}

#: Prime characters.
PRIMES = {
    "′",  # \u2032
    "″",  # \u2033
    "‴",  # \u2034
    "‵",  # \u2035
    "‶",  # \u2036
    "‷",  # \u2037
    "⁗",  # \u2057
}

#: Quote characters, including apostrophes, single quotes, double quotes, accents and primes.
QUOTES = APOSTROPHES | SINGLE_QUOTES | DOUBLE_QUOTES | ACCENTS | PRIMES


class BaseNormalizer(ABC):
    """Abstract normalizer class from which all normalizers inherit.

    Subclasses must implement a ``normalize()`` method.
    """

    @abstractmethod
    def normalize(self, text):
        """Normalize the text.

        :param string text: The text to normalize.
        :returns: Normalized text.
        :rtype: string
        """
        return text

    def __call__(self, text):
        """Calling a normalizer instance like a function just calls the normalize method."""
        return self.normalize(text)


class Normalizer(BaseNormalizer):
    """Main Normalizer class for generic English text.

    Normalize unicode, hyphens, quotes, whitespace.

    By default, the normal form NFKC is used for unicode normalization. This applies a compatibility decomposition,
    under which equivalent characters are unified, followed by a canonical composition. See Python docs for information
    on normal forms: http://docs.python.org/2/library/unicodedata.html#unicodedata.normalize
    """

    def __init__(
        self,
        form="NFKC",
        strip=True,
        collapse=True,
        hyphens=False,
        quotes=False,
        ellipsis=False,
        slashes=False,
        tildes=False,
    ):
        """

        :param string form: Normal form for unicode normalization.
        :param bool strip: Whether to strip whitespace from start and end.
        :param bool collapse: Whether to collapse all whitespace (tabs, newlines) down to single spaces.
        :param bool hyphens: Whether to normalize all hyphens, minuses and dashes to the ASCII hyphen-minus character.
        :param bool quotes: Whether to normalize all apostrophes, quotes and primes to the ASCII quote character.
        :param bool ellipsis: Whether to normalize ellipses to three full stops.
        :param bool slashes: Whether to normalize slash characters to the ASCII slash character.
        :param bool tildes: Whether to normalize tilde characters to the ASCII tilde character.
        """
        self.form = form
        self.strip = strip
        self.collapse = collapse
        self.hyphens = hyphens
        self.quotes = quotes
        self.ellipsis = ellipsis
        self.slashes = slashes
        self.tildes = tildes

    def normalize(self, text):
        """Run the Normalizer on a string.

        :param text: The string to normalize.
        """
        # Normalize to canonical unicode (using NFKC by default)
        if self.form is not None:
            text = unicodedata.normalize(self.form, text)

        # Strip out any control characters (they occasionally creep in somehow)
        for control in CONTROLS:
            text = text.replace(control, "")

        # Normalize unusual whitespace not caught by unicodedata
        # text = text.replace('\u000b', ' ').replace('\u000c', ' ').replace(u'\u0085', ' ')
        text = text.replace("\u000b", " ").replace(u"\u0085", " ")
        text = (
            text.replace("\u2028", "\n")
            .replace("\u2029", "\n")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
        )

        # Normalize all hyphens, minuses and dashes to ascii hyphen-minus and remove soft hyphen entirely
        if self.hyphens:
            # TODO: Better normalization of em/en dashes to '--' if surrounded by spaces or start/end?
            for hyphen in HYPHENS | MINUSES:
                text = text.replace(hyphen, "-")
            text = text.replace("\u00ad", "")

        # Normalize all quotes and primes to ascii apostrophe and quotation mark
        if self.quotes:
            for double_quote in DOUBLE_QUOTES:
                text = text.replace(double_quote, '"')  # \u0022
            for single_quote in SINGLE_QUOTES | APOSTROPHES | ACCENTS:
                text = text.replace(single_quote, "'")  # \u0027
            text = text.replace("′", "'")  # \u2032 prime
            text = text.replace("‵", "'")  # \u2035 reversed prime
            text = text.replace("″", "''")  # \u2033 double prime
            text = text.replace("‶", "''")  # \u2036 reversed double prime
            text = text.replace("‴", "'''")  # \u2034 triple prime
            text = text.replace("‷", "'''")  # \u2037 reversed triple prime
            text = text.replace("⁗", "''''")  # \u2057 quadruple prime

        if self.ellipsis:
            text = text.replace("…", "...").replace(" . . . ", " ... ")  # \u2026

        if self.slashes:
            for slash in SLASHES:
                text = text.replace(slash, "/")

        if self.tildes:
            for tilde in TILDES:
                text = text.replace(tilde, "~")

        if self.strip:
            text = text.strip()

        # Collapse all whitespace down to a single space
        if self.collapse:
            pages = [x.strip() for x in text.split("\f")]
            text = "\f".join([" ".join(x.split()) for x in pages])
            # text = ' '.join(text.split())

        return text


refstring_normalizer = Normalizer(
    form="NFKD",
    strip=True,
    collapse=True,
    hyphens=True,
    quotes=True,
    slashes=True,
    tildes=True,
)
