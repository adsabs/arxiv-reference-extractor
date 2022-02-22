#! /usr/bin/env python

"""
Compare the resolved references from two Arxiv processing sessions. Usage:

    ./cmp-resolved.py <tagA> <tagB> <sessionid>

... where the <tags> are the names of two directories within $results_dir
and <sessionid> is the Arxiv update session name (e.g. 2021-11-07).

Requires $ADS_DEV_KEY environment variable to be set so that we can use the
reference resolver service if needed.
"""

import os.path as osp
from pathlib import Path
import sys

# Make sure we can find the Python package:
diagnostics_dir = osp.dirname(__file__)
app_dir = osp.join(diagnostics_dir, osp.pardir)
sys.path.append(app_dir)

from ads_ref_extract import config, classic_analytics, resolver_cache

if len(sys.argv) != 4:
    print("usage: ./repro.py <tagA> <tagB> <session-id>")
    sys.exit(1)

tagA = sys.argv[1]
tagB = sys.argv[2]
session_id = sys.argv[3]

no_rpc = False  # debugging setting

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

db_path = diagnostics_cfg["resolver_cache_db_path"]

with resolver_cache.ResolverCache(db_path) as rcache:
    cmp = classic_analytics.compare_resolved(
        session_id, cfgA, cfgB, rcache, no_rpc=no_rpc
    )

    for stem, info in cmp.items():
        if info.score_delta != 0:
            print(info)
            for lrs in info.lost_resolutions:
                print("       ", lrs)

    tot = 0

    for info in cmp.values():
        tot += info.score_delta

    print("N comparisons:", len(cmp))
    print("Total delta:", tot)
