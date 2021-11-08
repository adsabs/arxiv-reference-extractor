#!/bin/sh
#
# A simple wrapper around extractrefs.pl so we can run this
# as a one-off process for specific bibcodes
#
# Usage: extractrefs.sh [--debug] bibcode_or_arxivid [...]
#

script=`basename $0`

# writes an error message and exits
die () {
    echo "$script: fatal error: $1 occurred at " `date` 1>&2
    exit 1
}

warn () {
    echo "$script: $1 at " `date` 1>&2
}

bindir="$ADS_ABSTRACTS/sources/ArXiv/bin"
fdir="$ADS_ABSTRACTS/sources/ArXiv/fulltext"
opts=

while [ $# -ne 0 ]; do
    case "$1" in
        --debug)
            opts="$opts --debug --debug" ;;
        --help)
	    die "Usage: $script [--debug] [--force] bibcode_or_arxivid [...]
(use --debug twice to have the temporary files from reference extractions saved on disk)" ;;
        --*)
            opts="$opts $1" ;;
        *)
            break ;;
    esac
    shift
done

cd $fdir

# translate input ids into path, which is what extractrefs.pl wants
paths=$(perl -MADS::Abstracts::ArXiv -e '
    $topdir = "$ENV{ADS_ABSTRACTS}/sources/ArXiv/fulltext";
    @suff = qw( tar.gz pdf tex );
    print join(" ", map {
            $id = ADS::Abstracts::ArXiv::bib2id($_) || $_;
            $path = ADS::Abstracts::ArXiv::id2path($id);
            $file = ();
            foreach $s (@suff) {
                if (-f "$topdir/$path.$s") {
                    $file = ("$path.$s");
                    last;
                }
            }
            #warn "file is $file";
            $file
        } @ARGV)' "$@" )

for path in $paths; do
    warn "processing fulltext file $fdir/$path"
    echo $path
done | $bindir/extractrefs.pl $opts
