% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/files.R
\name{RawData}
\alias{RawData}
\title{Raw Data}
\usage{
RawData(datadir)
}
\arguments{
\item{datadir}{Directory where the data is stored.}
}
\value{
A \code{data.table} with columns type, source, repo and sub
  where type is the type of repository (e.g. git, jira or
  bugzilla), source is either Apache or Mozilla, repo is the name
  of the git repository or the jira/bugzilla product tag and sub is
  a unique id representing the file.
}
\description{
Returns the list of raw data files (JSON files). The data must be
stored in files named raw/<type>/<source>/<repo>.json(.gz) or
raw/<type>/<source>/<repo>/<sub>.json(.gz).
}
