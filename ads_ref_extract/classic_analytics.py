"""
Performance analytics on the logfiles produced by a "classic" Arxiv reference
extraction session.
"""

import logging

__all__ = ["ClassicSessionAnalytics", "analyze_session"]

default_logger = logging.getLogger(__name__)


def _split_item_path(item_path):
    """
    Convert an arxiv "item path" to its "stem" and "extension".

    The item path may be either a string of the form "arXiv/2110/08013.tar.gz",
    or the same with a filesystem prefix: "/proj/ads/.../fulltext/arXiv/...".
    The extension is not necessarily ".tar.gz"

    The "stem" of such a path is "arXiv/2110/08013", and the extension is
    "tar.gz". If there is no period in the basename of the item path, the
    extension is the empty string.
    """

    # Sometimes this is `arXiv/YYMM/NNNNN.EXT`, sometimes
    # `/proj/ads/.../fulltext/arXiv/YYMM/NNNNN.EXT`
    bits = item_path.split('fulltext/')
    if len(bits) > 1:
        item_path = bits[-1]

    bits = item_path.split('.', 1)
    stem = bits[0]

    if len(bits) > 1:
        ext = bits[1]
    else:
        ext = ''

    return (stem, ext)


class ClassicSessionAnalytics(object):
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

    n_reftexts = None
    "The total number of reference-text items emitted in the whole session."

    n_good_refs = None
    "The total number of references that were resolved to bibcodes with confidence, in the whole session."

    n_guess_refs = None
    "The total number of references that were resolved to bibcode guesses, in the whole session."

    def __str__(self):
        return f'''Classic session {self.session_id}:
    n_items = {self.n_items}
    n_new_items = {self.n_new_items}
    n_source_items = {self.n_source_items}
    n_emitted_items = {self.n_emitted_items}
    n_reftexts = {self.n_reftexts}
    n_good_refs = {self.n_good_refs}
    n_guess_refs = {self.n_guess_refs}'''

    def csv_header(self):
        return [
            'session_id', 
            'items', 
            'new_items', 
            'source_items', 
            'emitted_items', 
            'reftexts', 
            'good_refs', 
            'guess_refs',
        ]

    def as_csv_row(self):
        return [
            self.session_id,
            str(self.n_items),
            str(self.n_new_items),
            str(self.n_source_items),
            str(self.n_emitted_items),
            str(self.n_reftexts),
            str(self.n_good_refs),
            str(self.n_guess_refs),
        ]


def analyze_session(session_id, config, logger=default_logger):
    """
    Parse log files of a single processing session.
    """
    log_dir = config.classic_session_log_path(session_id)

    # First: analyze items that were in the update

    n_items = 0
    n_new = 0
    n_source = 0

    short_sid = session_id.replace('-', '')
    fth_path = log_dir / 'fulltextharvest.out'

    with open(fth_path, 'rt') as fth:
        for line in fth:
            bits = line.strip().split()
            if not bits:
                logger.warn(f'unexpected empty line in `{fth_path}`')
                continue

            n_items += 1
            _item_stem, item_ext = _split_item_path(bits[0])

            if item_ext in ('tar.gz', 'tex.gz'):
                n_source += 1
            elif item_ext in ('pdf',):
                pass
            else:
                logger.warn(f'unexpected Arxiv item source type `{item_ext}` in `{fth_path}`')

            if len(bits) > 3 and bits[3] == short_sid:
                n_new += 1

    # Next: analyze results of that update

    n_logged = 0
    n_emitted = 0
    raw_paths = {}

    er_path = log_dir / 'extractrefs.out'

    with open(er_path, 'rt') as er:
        for line in er:
            bits = line.strip().split()
            if not bits:
                logger.warn(f'unexpected empty line in `{er_path}`')
                continue

            n_logged += 1
            item_stem, item_ext = _split_item_path(bits[0])

            if len(bits) > 1:
                raw_paths[item_stem] = bits[1]
                n_emitted += 1

    # Next: analyze items that had reftext extracted

    n_reftexts = 0
    n_good_refs = 0
    n_guess_refs = 0

    for raw_path in raw_paths.values():
        # "reftext" extracted
        #
        # We have at least one case
        # (/proj/ads/references/sources/arXiv/2111/05148.raw) where this file is
        # not UTF-8, so let's avoid assuming that.

        try:
            with open(raw_path, 'rb') as raw_refs:
                for line in raw_refs:
                    if line.startswith(b'%Z'):
                        break

                for line in raw_refs:
                    if line.strip():
                        n_reftexts += 1
        except FileNotFoundError:
            logger.warn(f'unexpected missing ref target file `{raw_path}` for Arxiv session `{session_id}`')
            continue
        except Exception as e:
            logger.warn(
                f'exception parsing ref target file `{raw_path}` for Arxiv session `{session_id}`: {e} ({e.__class__.__name__})'
            )
            continue

        # Resolved

        resolved_path = raw_path.replace('sources/', 'resolved/') + '.result'

        try:
            with open(resolved_path, 'rb') as resolved_refs:
                resolved_refs.readline()  # skip bibcode/ID info

                for line in resolved_refs:
                    bits = line.strip().split()
                    if not bits:
                        logger.warn(f'unexpected empty line in `{resolved_path}`')
                        continue

                    if bits[0] == b'1':
                        n_good_refs += 1
                    elif bits[0] == b'5':
                        n_guess_refs += 1
        except FileNotFoundError:
            logger.warn(f'unexpected missing ref resolved file `{resolved_path}` for Arxiv session `{session_id}`')
            continue
        except Exception as e:
            logger.warn(
                f'exception parsing ref resolved file `{resolved_path}` for Arxiv session `{session_id}`: {e} ({e.__class__.__name__})'
            )
            continue

    # All done

    info = ClassicSessionAnalytics()
    info.session_id = session_id
    info.n_items = n_items
    info.n_new_items = n_new
    info.n_source_items = n_source
    info.n_emitted_items = n_emitted
    info.n_reftexts = n_reftexts
    info.n_good_refs = n_good_refs
    info.n_guess_refs = n_guess_refs
    return info
