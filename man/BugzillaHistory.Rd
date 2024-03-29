% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/bugzilla.R
\name{BugzillaHistory}
\alias{BugzillaHistory}
\title{Bugzilla History}
\usage{
BugzillaHistory(issue)
}
\arguments{
\item{issue}{List representing the issue (parsed from JSON).}
}
\value{
A \code{data.table} representing the history of change of the issue.
}
\description{
Parses history from an issue.
}
