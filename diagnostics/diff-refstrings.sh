#! /bin/bash
#
# Compare refstrings extracted for a single item.
#
#    ./diff-refstrings.sh <tagA> <tagB> <item>

tagA="$1"
shift
tagB="$1"
shift
item="$1"
shift

if [ -z "$item" ] ; then
    echo "usage: $0 <tagA> <tagB> <item>" >&2
    exit 1
fi

diagnostics_dir="$(dirname "$0")"
source $diagnostics_dir/diagnostics.cfg

# Validate setup

rawA=$results_dir/$tagA/references/sources/$item.raw
if [ ! -f $rawA ] ; then
    echo "fatal error: no such input \`$rawA\`" >&2
    exit 1
fi

rawB=$results_dir/$tagB/references/sources/$item.raw
if [ ! -f $rawB ] ; then
    echo "fatal error: no such input \`$rawB\`" >&2
    exit 1
fi

# Munge the refstring files so that the word diff doesn't wrap around lines in
# a distracting way

set -euo pipefail
workdir=$(mktemp -d)
sed -e 's/$/\n/' $rawA >$workdir/$tagA.raw
sed -e 's/$/\n/' $rawB >$workdir/$tagB.raw
git diff --no-index --word-diff=color $workdir/$tagA.raw $workdir/$tagB.raw
rm -rf $workdir
