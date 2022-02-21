#! /bin/bash
#
# Run the extractor app as a one-off Docker execution, customizing its
# environment to use local data directories.

diagnostics_dir="$(dirname "$0")"
source $diagnostics_dir/diagnostics.cfg

app="$(cd "$diagnostics_dir"/.. && pwd)"

if [ -z "$RESULTS_TAG" ] ; then
    this_results_dir="$results_dir/untagged"
else
    this_results_dir="$results_dir/$RESULTS_TAG"
fi

exec docker run --rm -i \
    -v $app:/app:ro,Z \
    -v $fulltext_dir:/fulltext:ro,Z \
    -v $this_results_dir:/results:rw,Z \
    -e ADS_ARXIVREFS_FULLTEXT=/fulltext \
    -e ADS_ARXIVREFS_LOGS=/results/logs \
    -e ADS_ARXIVREFS_REFOUT=/results/references/sources \
    $extractor_image \
    /app/run.py "$@"
