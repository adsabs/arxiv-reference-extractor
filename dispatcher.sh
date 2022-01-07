#! /bin/sh
#
# This dispatcher script is used as an entrypoint for the Docker container built
# from this app. It allows us to choose between using the classic Perl
# implementation and the new Python implementation on-the-fly, to make it easier
# to compare the performance of the two. It adds a couple of special argv[1] options:
#
# --impl-perl -- run the classic Perl extractor
# --impl-python -- run the Python extractor using the Perl-compatible interface
# --pymod $modname -- run the named Python submodule of ads_ref_extract.
#
# If no special arguments are given, the default behavior is like --impl-perl.

if [ "$1" = --pymod ] ; then
    shift
    cd "$(dirname "$0")"
    modname="$1"
    shift
    exec python3 -m "ads_ref_extract.$modname" "$@"
fi

if [ "$1" = --impl-python ] ; then
    shift
    cd "$(dirname "$0")"
    exec python3 -m ads_ref_extract.compat "$@"
fi

if [ "$1" = --impl-perl ] ; then
    shift
fi

exec perl "$(dirname "$0")"/classic/extractrefs.pl "$@"
