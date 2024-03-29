% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/log.R
\name{CommitLog}
\alias{CommitLog}
\title{Commit Log}
\usage{
CommitLog(git.input, issues.input, output = NULL)
}
\arguments{
\item{git.input}{Parquet file containing the raw git log.}

\item{issues.input}{Parquet file containing the raw issue log.}

\item{output}{Parquet file where to store the result.}
}
\value{
The output filename (if any) or the data.table object
  otherwise.
}
\description{
Processes raw git log by determinig time zone accuracy and linking
commits to issues.
}
