#! /bin/bash
#
# Unpack a single item into a temporary directory.

item="$1"
shift

if [ -z "$item" ] ; then
    echo "usage: $0 <item>" >&2
    exit 1
fi

diagnostics_dir="$(dirname "$0")"
source $diagnostics_dir/diagnostics.cfg

# Handle the fact that most of our fulltext samples have both .tar.gz and .pdf

source=

for f in $fulltext_dir/$item.* ; do
    if [ ! -f "$f" ] ; then
        continue
    fi

    case "$f" in
    *.pdf)
        if [ -z "$source" ] ; then
            source="$f"
        fi
        ;;

    *)
        source="$f"
    esac
done

if [ ! -f "$source" ] ; then
    echo "error: failed to evaluate $fulltext_dir/$item.*" >&2
    exit 1
fi

# OK, now actually handle it

case "$source" in
*.tar.gz|*.tgz)
    dir="$(mktemp -d)"
    tar xzvf "$source" -C "$dir" >&2
    echo "$dir"
    ;;

*.tex.gz)
    dir="$(mktemp -d)"
    b=$(basename "$source" .gz)
    zcat "$source" >$dir/$b
    echo "$dir/$b"
    ;;

*.tex | *.pdf)
    echo "$source"
    ;;

*)
    echo "error: unhandled extension: $source" >&2
    exit 1
esac
