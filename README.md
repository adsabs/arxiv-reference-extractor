# ADS ArXiV Reference Extraction Pipeline

A framework to extract bibliographic references from ArXiV submissions, derived
from the "classic" pipeline deployed in `/proj/ads/abstracts/sources/ArXiV/`.
The code is in the `ads_ref_extract/` Python package, with diagnostic tooling in
`diagnostics/`. See `diagnostics/README.md` for a description of those tools.


## Launching the pipeline

In ADS' online systems, the new pipeline is launched in a Docker container by
`docker exec`-ing the `run.py` script in "pipeline" mode. This delegates to
`ads_ref_extract/compat.py:entrypoint()`. Currently, processing is kicked off by
the classic backoffice script
`/proj/ads/abstracts/sources/ArXiv/bin/fulltext.sh`

In "pipeline" mode, the key interfaces with the environment are:

- The list of items to process is read from the `--pipeline PATH` argument.
- The "session ID" is determined from the directory name of the input path.
- Item fulltext files are read from `$ADS_ARXIVREFS_FULLTEXT`, or the default
  value of `$ADS_ABSTRACTS/sources/ArXiv/fulltext`.
- Output reference files are written inside `$ADS_ARXIVREFS_REFOUT`, which
  defaults to `$ADS_REFERENCES/sources`.
- Logs are written inside `$ADS_ARXIVREFS_LOGROOT`, in a subdirectory named
  with the session ID.

In the "non-pipeline" mode, which is compatible with the historical
`extractrefs.pl` script:

- The list of items to process is read from stdin.
- The "session ID" is unspecified.
- Fulltext inputs and reference outputs are as above.
- Summary logs are written to stdout, in a format compatible with the historical
  script.
- Additional logging information is written to stderr.

To test locally, we recommend using the framework found in the `diagnostics/`
subdirectory. Some modest configuration is required. You also need to have
copies of ArXiv data organized according to ADS' system in order for the
pipeline to be able to do anything useful.


## Configuration

Standard ADS environment variables are used as appropriate. Additional
environment variables are:

- `ADS_ARXIVREFS_FULLTEXT` - the base directory for Arxiv fulltext sources.
  Defaults to `$ADS_ABSTRACTS/sources/ArXiv/fulltext`.
- `ADS_ARXIVREFS_GROBID_HOST` - the hostname of the Grobid server, if
  Grobid-based PDF extraction is being used. Defaults to `localhost`.
- `ADS_ARXIVREFS_GROBID_PORT` - the port of the Grobid server, if Grobid-based
  PDF extraction is being used. Defaults to 8070.
- `ADS_ARXIVREFS_LOGROOT` - the base directory for log file outputs in
  `--pipeline` mode
- `ADS_ARXIVREFS_REFOUT` - the base directory for writing the new "target
  reference" files that represent the pipeline output. Defaults to
  `$ADS_REFERENCES/sources`.

Additionally, the `run.py` script accepts various command-line arguments
that can influence its behavior. Use `run.py --help` for a detailed listing.
Some options of note are:

- `--pdf-backend NAME` - The backend to use for extracting references from
  PDF files, when TeX-based extraction isn't successful. Valid settings for
  NAME are `perl` or `grobid`.
- `--no-tex` - Disable TeX extraction; use PDF-based extraction for all inputs.


## Dockerized classic pipeline

A Dockerized version of the classic pipeline is stored in the `classic/`
subdirectory. The goal of this variation is stick as closely as possible to the
classic pipeline, while making a few changes to be deployable in a containerized
setting.


## Maintainer(s)

- [@pkgw](https://github.com/pkgw)
