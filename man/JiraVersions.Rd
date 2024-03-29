% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/jira.R
\name{JiraVersions}
\alias{JiraVersions}
\title{Jira Versions}
\usage{
JiraVersions(issue)
}
\arguments{
\item{issue}{Issue as a list parsed from JSON.}
}
\value{
A \code{data.table} with releases associated with the
  issue.
}
\description{
Gets versions from a Jira issue.
}
