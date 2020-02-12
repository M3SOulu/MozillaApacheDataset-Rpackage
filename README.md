[![Build Status](https://travis-ci.org/M3SOulu/MozillaApacheDataset-Rpackage.svg?branch=master)](https://travis-ci.org/M3SOulu/MozillaApacheDataset-Rpackage)

# 20-MAD - MozillaApacheDataset (R Package)

This GitHub repository contains the R package to replicate the
processing pipeline of the 20-MAD dataset on raw data from Mozilla and
Apache code repositories and issue trackers.

## Data file structure

In order for it to work, raw data should be stored with the following
structure:
* Git: each Git repository raw data is either stored as a single JSON
  file raw/git/<source>/<repo>.json or as multiple JSON files in a
  folder raw/git/<source>/<repo>
* Jira: Apache's issue tracker raw data is stored inside
  raw/jira/apache. Individual folders contain issues specific to one
  product tag. For example raw/jira/apache/HADOOP contains all JSON
  files containing issues for Hadoop.
* Bugzilla: Mozilla's issue tracker raw data is stored inside
  raw/bugzilla/mozilla. Individual folders contain issues specific to
  one product tag. For example raw/bugzilla/mozilla/Firefox contains
  all JSON files containing issues for Firefox.

## Drake plans

The package relies on [drake](https://github.com/ropensci/drake) for
managing the workflow of the whole pipeline. It can be used to
automatically figure out which data files are up to date and which
needs to (re)generated in case of failure during execution.
