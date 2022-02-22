#! /usr/bin/env python

"""
Reprocess a daily Arxiv update. Usage:

    ./repro.py <tag> <sessionid>

... where <tag> is the name of the directory within `results_dir` where outputs
(logs and "target references" files) will be stored, and <sessionid> is the
Arxiv update session name (e.g. 2021-11-07).
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
    print("usage: ./repro.py <tag> <session-id>")
    sys.exit(1)

tag = sys.argv[1]
session_id = sys.argv[2]

diagnostics_cfg = config.parse_dumb_config_file(
    osp.join(diagnostics_dir, "diagnostics.cfg")
)
cfg = config.Config.new_defaults()
repro = classic_analytics.ClassicSessionReprocessor(config=cfg)

cfg.fulltext_base = Path(diagnostics_cfg["fulltext_dir"])
cfg.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{tag}/references/sources"
)
repro.image_name = diagnostics_cfg["extractor_image"]
repro.custom_app_dir = app_dir
repro.debug = True
repro.force = True

# These are the logs that we scan in order to figure out what to process:
cfg.logs_base = Path(f"{diagnostics_cfg['results_dir']}/prod/logs")
# These are the logs we'll create:
repro.logs_out_base = Path(f"{diagnostics_cfg['results_dir']}/{tag}/logs")

# OK, let's do it.
repro.reprocess(session_id)

cfg.logs_base = repro.logs_out_base
info = classic_analytics.analyze_session(
    session_id, cfg, reconstruct_targets=True, check_resolved=False
)
print(info)
