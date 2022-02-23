#! /usr/bin/env python3

"""
Compare the "reference strings" extracted in two processing sessions. Usage:

    ./cmp-refstrings.py <tagA> <tagB> <sessionid>

... where the <tags> are the names of two directories within $results_dir
and <sessionid> is the Arxiv update session name (e.g. 2021-11-07).
"""

import os.path as osp
from pathlib import Path
import sys

# Make sure we can find the Python package:
diagnostics_dir = osp.dirname(__file__)
app_dir = osp.join(diagnostics_dir, osp.pardir)
sys.path.append(app_dir)

from ads_ref_extract import config, classic_analytics

if len(sys.argv) != 4:
    print(f"usage: {sys.argv[0]} <tagA> <tagB> <session-id>")
    sys.exit(1)

tagA = sys.argv[1]
tagB = sys.argv[2]
session_id = sys.argv[3]

diagnostics_cfg = config.parse_dumb_config_file(
    osp.join(diagnostics_dir, "diagnostics.cfg")
)

cfgA = config.Config.new_defaults()
cfgA.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{tagA}/logs")
cfgA.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{tagA}/references/sources"
)

cfgB = config.Config.new_defaults()
cfgB.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{tagB}/logs")
cfgB.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{tagB}/references/sources"
)

for line in classic_analytics.compare_refstrings(session_id, cfgA, cfgB):
    print(line, end="")
