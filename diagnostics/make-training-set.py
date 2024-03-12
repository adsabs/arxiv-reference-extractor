#! /usr/bin/env python3

"""
Identify Arxiv preprints to go into a training set for PDF reference extraction.

We look for items where every single refstring resolves into a bibcode. Note
that this might require extra refstring resolutions compared to
`cmp-resolved.py`, since that script only resolves refstrings that *change*
between two processing sessions.
"""

import argparse
import logging
import os.path as osp
from pathlib import Path
import sys

# Make sure we can find the Python package:
diagnostics_dir = osp.dirname(__file__)
app_dir = osp.join(diagnostics_dir, osp.pardir)
sys.path.append(app_dir)

from ads_ref_extract import settings, classic_analytics, resolver_cache, utils

logger = utils.get_quick_logger("make-arxiv-training-set")

# Args

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--max-resolves",
    type=int,
    help="Maximum number of refstring resolutions to perform",
)
parser.add_argument("tag")
settings = parser.parse_args()

diagnostics_cfg = settings.parse_dumb_settings_file(
    osp.join(diagnostics_dir, "diagnostics.cfg")
)

cfg = settings.Settings.new_defaults()
cfg.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{settings.tag}/logs")
cfg.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{settings.tag}/references/sources"
)

db_path = diagnostics_cfg["resolver_cache_db_path"]

# Get all of the stems

stems = {}

for session_id in cfg.logs_base.iterdir():
    er = cfg.classic_session_log_path(session_id) / "extractrefs.out"

    for stem, ext, tr_path in classic_analytics._target_refs_for_session(
        er, True, cfg, classic_analytics.default_logger
    ):
        if tr_path is not None:
            stems[stem] = tr_path

# Find out which ones are "perfect".

n_refs_resolved = 0
n_checked = 0
n_accepted = 0

with resolver_cache.ResolverCache(db_path) as rcache:
    for stem, tr_path in stems.items():
        refstrings = classic_analytics._maybe_load_raw_file(
            tr_path, classic_analytics.default_logger
        )

        # Once we hit the maximum number of resolves, still check
        # any items for which no resolutions are needed.
        nr = rcache.count_need_rpc(refstrings)
        if nr != 0 and n_refs_resolved + nr > settings.max_resolves:
            continue

        n_checked += 1

        if not refstrings:
            continue

        any_bad = False

        for rs, ri in rcache.resolve(
            refstrings, logger=logger, level=logging.DEBUG - 1
        ).items():
            if ri.score <= classic_analytics.SUCCESSFUL_RESOLUTION_THRESHOLD:
                any_bad = True
                break

        n_refs_resolved += nr

        if any_bad:
            continue

        print(stem)
        n_accepted += 1

print()
print(f">>> {len(stems)} items considered")
print(f">>> {n_checked} had all resolutions available")
print(f">>> {n_accepted} accepted ({100 * n_accepted / n_checked:.1f}%)")
