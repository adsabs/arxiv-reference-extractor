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

from ads_ref_extract import ref_extract_paths, classic_analytics, resolver_cache

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

with resolver_cache.ResolverCache(db_path) as rcache:
    cmp = classic_analytics.compare_resolved(
        settings.session_id,
        cfgA,
        cfgB,
        rcache,
        no_rpc=no_rpc,
        max_resolves=settings.max_resolves,
    )

    n_tried_A = 0
    n_tried_B = 0
    n_succeeded_A = 0
    n_succeeded_B = 0
    n_strings_A = 0
    n_strings_B = 0
    subsample_A_score = 0
    subsample_delta = 0

    print(
        "{:40}  {:12}  {:>4}  {:>8}  {:>6}  {:>6}  {:>6}".format(
            "ITEM", "EXT-A/B", "NR_A", "NR_(B-A)", "NLOST", "NGAIN", "DSCORE"
        )
    )

    for stem, info in sorted(
        cmp.items(), key=lambda kv: kv[1].score_delta, reverse=True
    ):
        ext = f"{info.A_ext}/{info.B_ext}"
        print(
            f"{stem:40}  {ext:12}  {info.n_strings_A:4d}  {info.n_strings_B - info.n_strings_A:+8d}  {info.n_lost:6d}  {info.n_gained:6d}  {info.score_delta:+6.1f}"
        )
        n_tried_A += info.n_tried_A
        n_tried_B += info.n_tried_B
        n_succeeded_A += info.n_succeeded_A
        n_succeeded_B += info.n_succeeded_B
        n_strings_A += info.n_strings_A
        n_strings_B += info.n_strings_B
        subsample_A_score += info.A_score
        subsample_delta += info.score_delta

    # We'll assume that the reference resolution rate for the changed refstrings
    # is not systematically different than that for unchanged refstrings.

    rA = n_succeeded_A / n_tried_A
    rB = n_succeeded_B / n_tried_B

    print()
    print(f"N compared items: {len(cmp)}")
    print(f"N refstrings in common: {n_strings_A - n_tried_A}")

    print()
    print(f"Total number of refstrings in A sample: {n_strings_A}")
    print(f"    Size of subsample sent to resolver: {n_tried_A}")
    print(f"         Number that actually resolved: {n_succeeded_A} (rate: {rA:.2f})")
    print()
    print(f"Total number of refstrings in B sample: {n_strings_B}")
    print(f"    Size of subsample sent to resolver: {n_tried_B}")
    print(f"         Number that actually resolved: {n_succeeded_B} (rate: {rB:.2f})")

    sample_A_score_est = subsample_A_score * n_strings_A / n_tried_A
    print()
    print(f"Total score delta: {subsample_delta:+.1f}")
    print(
        f"Total A subsample score: {subsample_A_score:+.1f} (fractional improvement in B: {subsample_delta / subsample_A_score:+.3f})"
    )
    print(
        f"Estimated A sample score: {sample_A_score_est:+.1f} (fractional improvement in B: {subsample_delta / sample_A_score_est:+.3f})"
    )
    print(f"Estimated A bibcodes: {n_strings_A * rA:.0f}")
    print(f"Estimated B bibcodes: {n_strings_B * rB:.0f}")
