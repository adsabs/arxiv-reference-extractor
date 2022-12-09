#! /usr/bin/env python3

"""
Reprocess a daily Arxiv update. Usage:

    ./repro.py [--ref <reftag>] <tag> <sessionid>

... where <tag> is the name of the directory within `results_dir` where outputs
(logs and "target references" files) will be stored, and <sessionid> is the
Arxiv update session name (e.g. 2021-11-07).

<reftag> is the "reference" tag, which is the template source used to identify
which items to process. It defaults to `prod`.

Set the environment variable $REPRO_ARGS to send additional options to the
pipeline processing program.
"""

import argparse
import os
import os.path as osp
from pathlib import Path
import sys

# Make sure we can find the Python package:
diagnostics_dir = osp.dirname(__file__)
app_dir = osp.join(diagnostics_dir, osp.pardir)
sys.path.append(app_dir)

from ads_ref_extract import config, classic_analytics

parser = argparse.ArgumentParser()
parser.add_argument(
    "--ref",
    dest="ref_tag",
    default="prod",
    help="Tag to use as a reference",
)
parser.add_argument("tag")
parser.add_argument("session_id")
settings = parser.parse_args()

diagnostics_cfg = config.parse_dumb_config_file(
    osp.join(diagnostics_dir, "diagnostics.cfg")
)
cfg = config.Config.new_defaults()
repro = classic_analytics.ClassicSessionReprocessor(config=cfg)

cfg.fulltext_base = Path(diagnostics_cfg["fulltext_dir"])
cfg.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{settings.tag}/references/sources"
)
repro.image_name = diagnostics_cfg["extractor_image"]
repro.custom_app_dir = app_dir
repro.debug = True
repro.force = True

extra_args_text = os.environ.get("REPRO_ARGS")
if extra_args_text:
    repro.extra_args = extra_args_text.split()

# These are the logs that we scan in order to figure out what to process:
cfg.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{settings.ref_tag}/logs")
# These are the logs we'll create:
repro.logs_out_base = Path(f"{diagnostics_cfg['results_dir']}/{settings.tag}/logs")

# OK, let's do it.
repro.reprocess(settings.session_id)

cfg.logs_base = repro.logs_out_base
info = classic_analytics.analyze_session(
    settings.session_id, cfg, reconstruct_targets=True, check_resolved=False
)
print(info)
