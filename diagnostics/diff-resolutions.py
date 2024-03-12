#! /usr/bin/env python3

"""
Print a "diff"-style output of the refstrings for an item based on different
bibcode resolutions

    ./diff-resolutions.py <tagA> <tagB> <item>

Requires $ADS_DEV_KEY environment variable to be set so that we can use the
reference resolver service if needed.
"""

import argparse
import os.path as osp
from pathlib import Path
import sys

# Make sure we can find the Python package:
diagnostics_dir = osp.dirname(__file__)
app_dir = osp.join(diagnostics_dir, osp.pardir)
sys.path.append(app_dir)

from ads_ref_extract import ref_extract_paths, classic_analytics, resolver_cache

# Args

parser = argparse.ArgumentParser()
parser.add_argument("tag_a")
parser.add_argument("tag_b")
parser.add_argument("stem")
settings = parser.parse_args()

# Set up everything

diagnostics_cfg = ref_extract_paths.parse_dumb_paths_file(
    osp.join(diagnostics_dir, "diagnostics.cfg")
)

cfgA = ref_extract_paths.Filepaths.new_defaults()
cfgA.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{settings.tag_a}/logs")
cfgA.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{settings.tag_a}/references/sources"
)

cfgB = ref_extract_paths.Filepaths.new_defaults()
cfgB.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{settings.tag_b}/logs")
cfgB.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{settings.tag_b}/references/sources"
)

db_path = diagnostics_cfg["resolver_cache_db_path"]

# Resolve and analyze!

with resolver_cache.ResolverCache(db_path) as rcache:
    info = classic_analytics.compare_item_resolutions(settings.stem, cfgA, cfgB, rcache)

print(f"Number of refstrings in `{settings.tag_a}`: {info.n_strings_A}")
print(f"Number of refstrings in `{settings.tag_b}`: {info.n_strings_B}")
print(f"B-A score delta: {info.score_delta:+.1f}")
print(
    f'Number of "lost" bibcodes (in `{settings.tag_a}` not in `{settings.tag_b}`):',
    info.n_lost,
)
print(
    f'Number of "gained" bibcodes (in `{settings.tag_b}` not in `{settings.tag_a}`):',
    info.n_gained,
)

print()
print("Lost bibcode analysis -- resolved by A, closest unresolved string is B")

for bib, (rs_A, distance, rs_B) in info.lost_bibcode_guesses.items():
    print(f"   {bib} (distance {distance}):")
    print("      A:", rs_A)
    print("      B:", rs_B)
