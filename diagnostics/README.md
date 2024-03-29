# ArXiv Reference Extractor: Diagnostic Infrastructure

This directory contains some scripts for running diagnostics on the performance
of the ArXiv reference extractor, including local testing and prototyping.


## Concepts

Some basic terminology used in the ArXiv reference extraction framework:

- A *session* is a processing run of a batch of Arxiv updates. These occur daily,
  so sessions are uniquely identifed using dates, e.g. `2021-11-07`.
- An *item* is a single Arxiv document, identified as something like
  `arXiv/1904/09850` or (don't forget the old days!) `astro-ph/1992/9204001`.
  Items can be processed in multiple sessions because they can get updated.
- Extraction of an item produces "reference strings" (or *refstrings*), which
  are bibliographic references in an unstructured, textual form.
- Refstrings can be *resolved* to specific identifiers (that is, ADS bibcodes)
  by an external ADS API service.

In this diagnostics kit:

- We can "reprocess" different sessions using the framework to produce (1)
  diagnostic logs and (2) refstrings for all of the items in a session.
- Each reprocessing run is labeled with a textual *tag*. The results are
  organized according to tag, so that you can reprocess the same session multiple
  times and compare the outcomes.


## Configuration and Setup

This diagnostics kit requires some setup before you can do anything with it.

To configure, copy the file `diagnostics.cfg.tmpl` to the name `diagnostics.cfg`
and edit the contents as instructed therein. The file must be source-able in a
Bourne shell without any quotation marks. It is parsed by both shell scripts and
Python scripts.

In order to reprocess any sessions, set up your `results_dir` with a tag called
`prod`, populated with logs of the production ArXiv update runs you're
interested in reprocessing. That is, if you want to reprocess the session
`2021-11-07`, the following file should exist:
`$results_dir/prod/logs/2021/2021-11-07/fulltextharvest.out`. You might be able
to use a symlink for this.

If you'll want to compare refstring outputs to the production system, you'll
need to likewise set up `prod` data for the processed items, e.g.:
`$results_dir/prod/references/sources/arXiv/1904/09850.raw`. Note that we don't
compare against "resolved" production references since those are resolved using
an older resolver service, and we want an apples-to-apples comparison.


## Commands

The utilities are as follows:

### ./unpack.sh {ITEM}

Given an ArXiv item ID (e.g. "arXiv/1904/09850"), unpack its fulltext source
into a temporary directory, printing the directory's name.

### ./oneoff.sh {ITEM}

Do a one-off extraction of the specified ArXiv item, placing results under the
tag "oneoffs".

### ./repro.py {TAG} {SESSION}

Reprocess the given ArXiv session (e.g. "2021-11-07"), placing logfiles and
reference files into the results directory under the specified tag. A results
tag named "prod" must exist; its associated logs will be used as the
source-of-truth about which items should be processed (as well as their
bibcodes).

If set, the environment variable `$REPRO_ARGS` can be used to pass extra
command-line arguments to the processing tool, which is run inside a Docker
container.

On @pkgw's laptop, reprocessing averages about 2 seconds per item, which means
that reprocessing usually takes about 45 minutes for a typical session.

### ./summarize.py {TAG} {SESSION}

Print some summary statistics about the outcomes of the specified processing
session, based on the results with the specified tag.

### ./logs.sh {TAG} {SESSION} {ITEM}

Print out the primary processing logs associated with the given item as
processed in the specific session+tag. This only works for sessions processed
using the new Python-based framework.

### ./cmp-outcomes.py {TAG-A} {TAG-B} {SESSION}

Print a comparison of the item-level outcomes for two different processing runs
of the same session. This doesn't compare the reference strings that were
extracted but does compare how many items were successfully processed, if any
regressed in B relative to A, etc.

### ./cmp-refstrings.py [--diff] {TAG-A} {TAG-B} {SESSION}

Print a comparison of the reference strings extracted from two different
processing runs of the same session. If `--diff` is given, this includes a
"diff"-like output for each item (which will usually be voluminous).

### ./diff-refstrings.sh {TAG-A} {TAG-B} {ITEM}

Compare the extracted refstrings of two processings of an individual item. This
uses a colorized, word-level diff display.

### ./cmp-resolved.py [-m {MAX-RESOLVES}] {TAG-A} {TAG-B} {SESSION}

Compare the resolved references extracted from two different processing runs of
the same session. This relies on API calls to the ADS reference resolver
service, which (1) are not very fast and (2) are rate-limited. Therefore we use
a local "reference resolver cache" which caches resolution results for input
refstrings. This doesn't speed up initial processing, but dramatically improves
results when only small changes are made. The `-m` option limits the number of
analyzed items to keep the number of API calls below the specified threshold.
This is done using a quasi-random but repeatable sorting of the input items, so
that successive invocations will analyze progressively larger subsets of the
whole session.

If any API calls need to be made (which is almost always true), you must set the
environment variable `$ADS_DEV_KEY` to an API token. Resolution for a single
Arxiv processing session can take *hours* if most of the references aren't
cached. (The microservice averages about 1.2 seconds per refstring.) While
transient network errors can also kill the analysis program, if you rerun it, it
will resume where it left off, thanks to the local cache.

### ./diff-resolutions.py {TAG-A} {TAG-B} {ITEM}

Compare the resolved references extracted from two different processings of the
specified item. This will perform reference resolver API calls if needed.

This will print an analysis of "lost" bibcodes: bibcodes that were identified in
the "A" tag but not in the "B" tag. For each such bibcode, the tool will print
out the refstring in A that resolved to it, and the *unresolved* refstring in B
that is closest to that refstring, as quantified by the Levenshtein edit
distance.

### ./run-it.sh {DRIVER-ARGS...}

A helper script that runs the extraction pipeline inside a Docker container. The
parent of the directory containing this script is mounted as `/app` in the
container, meaning that local modifications *will* take effect. This is only
invoked by `oneoff.sh`. The `repro.py` script uses the
`ClassicSessionReprocessor` framework found in
`ads_ref_extract.classic_analytics`.

### ./dump-resolutions.py {PATHS...}

The input files are "target reference" files of the format found in
`$results_dir/references/sources/`. This utility resolves the refstrings in
those files and prints out their resolutions, which can be helpful for
understanding if/how a particular refstring is resolving successfully or not.

If any API calls need to be made, the environment variable `$ADS_DEV_KEY` must
be set as above.

### ./make-training-set.py [-m MAX-RESOLVES] {TAG}

Identify Arxiv submissions to go into a training set for ADS' reference
extraction tools.

In particular, this script prints out the identifiers of items in the specified
tag where every single refstring successfully resolves into a bibcode. The
assumption is that if this is the case, the (presumably TeX-based) extraction
went well and the associated information should make for a good test case for
PDF-based extraction. This filter will obviously yield a very biased subset of
the inputs, but that's not necessarily a problem. The search is done over every
item in every session, so that tag's results should probably include only a
relatively small number of items. In the initial processing, about 2% of Arxiv
items meet the criterion.

This will likely need to invoke the reference resolver because the default
analysis only looks at *changed* refstrings between two samples, while this
analysis needs to resolve everything.