#! /usr/bin/env python3

"""
Compare the resolved references from two Arxiv processing sessions. Usage:

    ./cmp-resolved.py [-m <maxresolves>] <tagA> <tagB> <sessionid>

... where the <tags> are the names of two directories within $results_dir
and <sessionid> is the Arxiv update session name (e.g. 2021-11-07).

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

from ads_ref_extract import config, classic_analytics, resolver_cache

# Args

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--max-resolves",
    type=int,
    help="Maximum number of refstring resolutions to perform",
)
parser.add_argument("tag_a")
parser.add_argument("tag_b")
parser.add_argument("session_id")
settings = parser.parse_args()

no_rpc = False  # debugging setting

diagnostics_cfg = config.parse_dumb_config_file(
    osp.join(diagnostics_dir, "diagnostics.cfg")
)

cfgA = config.Config.new_defaults()
cfgA.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{settings.tag_a}/logs")
cfgA.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{settings.tag_a}/references/sources"
)

cfgB = config.Config.new_defaults()
cfgB.logs_base = Path(f"{diagnostics_cfg['results_dir']}/{settings.tag_b}/logs")
cfgB.target_refs_base = Path(
    f"{diagnostics_cfg['results_dir']}/{settings.tag_b}/references/sources"
)

db_path = diagnostics_cfg["resolver_cache_db_path"]

with resolver_cache.ResolverCache(db_path) as rcache:
    cmp = classic_analytics.compare_resolved(
        settings.session_id,
        cfgA,
        cfgB,
        rcache,
        no_rpc=no_rpc,
        max_resolves=settings.max_resolves,
    )

    print(
        "{:20}  {:12}  {:>4}  {:>8}  {:>6}  {:>6}  {:>6}".format(
            "ITEM", "EXT-A/B", "NR_A", "NR_(B-A)", "NLOST", "NGAIN", "DSCORE"
        )
    )

    for stem, info in sorted(
        cmp.items(), key=lambda kv: kv[1].score_delta, reverse=True
    ):
        ext = f"{info.A_ext}/{info.B_ext}"
        print(
            f"{stem:20}  {ext:12}  {info.n_strings_A:4d}  {info.n_strings_B - info.n_strings_A:+8d}  {info.n_lost:6d}  {info.n_gained:6d}  {info.score_delta:+6.1f}"
        )

    print()
    tot = 0

    for info in cmp.values():
        tot += info.score_delta

    print("N comparisons:", len(cmp))
    print(f"Total delta: {tot:+.1f}")
