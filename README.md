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

To test locally, we recommend using the framework found in the `diagnostics/`
subdirectory. Some modest configuration is required. You also need to have
copies of ArXiv data organized according to ADS' system in order for the
pipeline to be able to do anything useful.

## Maintainer(s)

- [@pkgw](https://github.com/pkgw)
