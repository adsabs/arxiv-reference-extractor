#! /usr/bin/env python3

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

from ads_ref_extract import ref_extract_paths, classic_analytics

if len(sys.argv) != 3:
    print(f"usage: {sys.argv[0]} <tag> <session-id>")
    sys.exit(1)

tag = sys.argv[1]
session_id = sys.argv[2]

diagnostics_cfg = ref_extract_paths.parse_dumb_paths_file(
    osp.join(diagnostics_dir, "diagnostics.cfg")
)
cfg = ref_extract_paths.Filepaths.new_defaults()
cfg.fulltext_base = Path(diagnostics_cfg["fulltext_dir"])
cfg.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{tag}/logs")
cfg.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{tag}/references/sources"
)

resolved_refs = Path(str(cfg.target_refs_base).replace("sources", "resolved"))
check_resolved = resolved_refs.exists()

info = classic_analytics.analyze_session(
    session_id, cfg, reconstruct_targets=True, check_resolved=check_resolved
)
print(info)
