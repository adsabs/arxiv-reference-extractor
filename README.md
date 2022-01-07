# ADS ArXiV Reference Extraction Pipeline

A framework to extract bibliographic references from ArXiV submissions, derived
from the "classic" pipeline deployed in `/proj/ads/abstracts/sources/ArXiV/`.

## Dockerized classic pipeline

A Dockerized version of the classic pipeline is stored in the `classic/`
subdirectory. The goal of this variation is stick as closely as possible to the
classic pipeline, while making a few changes to be deployable in a containerized
setting.

## New Python implementation

A modernized version of the pipeline is under construction in the
`ads_ref_extract/` Python package. At the moment, most of the code there has
to do with parsing pipeline logs to extract analytics about its performance.

## Launching the pipeline

To unit-test in the Docker container, use commands like:

```
$ ./localtest.sh --pymod tex extract /proj/ads/abstracts/sources/ArXiv/fulltext/arXiv/2111/03186.tar.gz
```

To test the compatibility interface:

```
$ echo 'arXiv/2111/03160.tar.gz 2021arXiv211103160S X18-82393 20211107' \
  |./localtest.sh --impl-python --debug --force --tbase /refout/pytest/sources
```

## Maintainer(s)

- [@pkgw](https://github.com/pkgw)
