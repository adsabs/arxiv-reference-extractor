#! /usr/bin/env python

"""
Summarize the results of a single Arxiv processing session. Usage:

    ./summarize.py <tag> <sessionid>

... where <tag> is the name of the directory within `results_dir` and
<sessionid> is the Arxiv update session name (e.g. 2021-11-07).
"""

import os.path as osp
from pathlib import Path
import sys

# Make sure we can find the Python package:
diagnostics_dir = osp.dirname(__file__)
app_dir = osp.join(diagnostics_dir, osp.pardir)
sys.path.append(app_dir)

from ads_ref_extract import config, classic_analytics

if len(sys.argv) != 3:
    print("usage: ./summarize.py <tag> <session-id>")
    sys.exit(1)

tag = sys.argv[1]
session_id = sys.argv[2]

diagnostics_cfg = config.parse_dumb_config_file(
    osp.join(diagnostics_dir, "diagnostics.cfg")
)
cfg = config.Config.new_defaults()
cfg.fulltext_base = Path(diagnostics_cfg["fulltext_dir"])
cfg.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{tag}/logs")
cfg.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{tag}/references/sources"
)
cfg.resolved_refs_base = Path(str(cfg.target_refs_base).replace("sources", "resolved"))

check_resolved = cfg.resolved_refs_base.exists()

info = classic_analytics.analyze_session(
    session_id, cfg, reconstruct_targets=True, check_resolved=check_resolved
)
print(info)
