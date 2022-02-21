#! /bin/bash
#
# Get logs for an item in a processing run.

tag="$1"
session="$2"
item="$3"

if [ -z "$item" ] ; then
    echo "usage: $0 <tag> <session> <item>" >&2
    exit 1
fi

diagnostics_dir="$(dirname "$0")"
source $diagnostics_dir/diagnostics.cfg

year="$(echo $session |cut -d- -f1)"
exec grep "%.* $item" $results_dir/$tag/logs/$year/$session/extractrefs.stderr
