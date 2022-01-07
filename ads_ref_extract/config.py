"""
A simple configuration abstraction to hopefully make it easy to use these
modules in both standalone CLIs and the standard Celery environment.
"""

import os
from pathlib import Path

__all__ = ["Config"]


class Config(object):
    logs_base: Path = None
    fulltext_base: Path = None
    target_refs_base: Path = None
    resolved_refs_base: Path = None
    abstracts_config_base: Path = None
    abstracts_links_base: Path = None
    tex_bin_dir: Path = None

    @classmethod
    def new_defaults(cls):
        """
        Create a new Config with default paths set for ADS infra.
        """

        abstracts = Path(os.environ.get('ADS_ABSTRACTS', '/proj/ads/abstracts'))
        references = Path(os.environ.get('ADS_REFERENCES', '/proj/ads/references'))

        inst = cls()
        inst.logs_base = abstracts / "sources" / "ArXiv" / "log"

        # NB: this must end in the string `fulltext` in order for some of the
        # log-parsing code to work correctly.
        inst.fulltext_base = abstracts / "sources" / "ArXiv" / "fulltext"

        # NB: These must be the same but with `sources` replaced with
        # `resolved`.
        inst.target_refs_base = references / "sources"
        inst.resolved_refs_base = references / "resolved"

        inst.abstracts_config_base = abstracts / "config"
        inst.abstracts_links_base = abstracts / "links"

        # This assumes that we're running in the standard Docker container:
        inst.tex_bin_dir = Path("/src/tex/bin/x86_64-linux")
        return inst

    def classic_session_log_path(self, session_id):
        """
        Get the path to the logs directory for a "classic" Arxiv processing
        session.

        ``session_id`` should be a string resembling "2021-01-31".

        The return value is a Path object pointing to the directory containing
        the different logfiles for the session, such as ``extractrefs.out``,
        ``fulltextharvest.input``, etc. This function doesn't confirm that the
        logs directory actually exists.
        """

        # Recent sessions are at the toplevel
        p = self.logs_base / session_id
        if p.is_dir():
            return p

        # Older sessions are archived by year.
        return self.logs_base / session_id.split("-")[0] / session_id
