#! /bin/bash
#
# Do a one-off processing of a single item.
#
# The results tag will be "oneoffs".
#
# You can specify extra arguments to the processing script after the two
# required arguments. --debug and --force are always provided.

item="$1"
shift

if [ -z "$item" ] ; then
    echo "usage: $0 <item>" >&2
    exit 1
fi

diagnostics_dir="$(dirname "$0")"
source $diagnostics_dir/diagnostics.cfg

# The stdin extraction is gross but we'll see how long we can get away with it.

echo "running with args: --debug --force $@"
echo

find $results_dir/*/logs -name fulltextharvest.out \
  |xargs grep -h "^$item" \
  |head -n1 \
  |RESULTS_TAG=oneoffs $diagnostics_dir/run-it.sh --debug --force "$@"
