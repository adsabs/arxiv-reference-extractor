#! /usr/bin/env python3

"""
Compare the "reference strings" extracted in two processing sessions. Usage:

    ./cmp-refstrings.py <settings.tag_a> <settings.tag_b> <sessionid>

... where the <tags> are the names of two directories within $results_dir
and <sessionid> is the Arxiv update session name (e.g. 2021-11-07).
"""

import argparse
import os.path as osp
from pathlib import Path
import sys

# Make sure we can find the Python package:
diagnostics_dir = osp.dirname(__file__)
app_dir = osp.join(diagnostics_dir, osp.pardir)
sys.path.append(app_dir)

from ads_ref_extract import ref_extract_paths, classic_analytics

# Args

parser = argparse.ArgumentParser()
parser.add_argument(
    "--diff",
    "-d",
    action="store_true",
    help="Include detailed diff-like output",
)
parser.add_argument("tag_a")
parser.add_argument("tag_b")
parser.add_argument("session_id")
settings = parser.parse_args()

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

for line in classic_analytics.compare_refstrings(
    settings.session_id, cfgA, cfgB, show_diff=settings.diff
):
    print(line, end="")
