#! /bin/bash
#
# Run the extractor app as a one-off Docker execution, customizing its
# environment to use local data directories.

diagnostics_dir="$(dirname "$0")"
source $diagnostics_dir/diagnostics.cfg

app="$(cd "$diagnostics_dir"/.. && pwd)"

if [ -z "$RESULTS_TAG" ] ; then
    this_refout_dir="$results_dir/untagged/references/sources"
else
    this_refout_dir="$results_dir/$RESULTS_TAG/references/sources"
fi

exec docker run --rm -i \
    -v $app:/app:ro,Z \
    -v $fulltext_dir:/fulltext:ro,Z \
    -v $this_refout_dir:/refout:rw,Z \
    -e ADS_ARXIVREFS_FULLTEXT=/fulltext \
    -e ADS_ARXIVREFS_REFOUT=/refout \
    $extractor_image \
    /app/run.py "$@"
