#! /usr/bin/env python3

"""
Dump refstring resolutions.

    ./dump-resolutions.py <file1> [file2...]

Requires $ADS_DEV_KEY environment variable to be set so that we can use the
reference resolver service if needed.
"""

import os.path as osp
import sys

# Make sure we can find the Python package:
diagnostics_dir = osp.dirname(__file__)
app_dir = osp.join(diagnostics_dir, osp.pardir)
sys.path.append(app_dir)

from ads_ref_extract import settings, resolver_cache

if len(sys.argv) < 2:
    print(f"usage: {sys.argv[0]} <file1> [file2...]")
    sys.exit(1)

paths = sys.argv[1:]

diagnostics_cfg = settings.parse_dumb_settings_file(
    osp.join(diagnostics_dir, "diagnostics.cfg")
)

db_path = diagnostics_cfg["resolver_cache_db_path"]


def simple_filter(path):
    with open(path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("%"):
                continue

            yield line


first = True

with resolver_cache.ResolverCache(db_path) as rcache:
    for path in paths:
        refstrings = list(simple_filter(path))
        resolved = rcache.resolve(refstrings)

        if first:
            first = False
        else:
            print()

        print(f"================ {path} ================")

        for rs in refstrings:
            print(f"{rs[:40]} => {resolved[rs]}")
