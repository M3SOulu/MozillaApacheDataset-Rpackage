% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/git.R
\name{ParseGitLog}
\alias{ParseGitLog}
\title{Parse Git log.}
\usage{
ParseGitLog(source, repo, log, max.message.length = NA)
}
\arguments{
\item{source}{Source of the repository (e.g. Apache or Mozilla).}

\item{repo}{Name of the Git repository.}

\item{log}{List of commits parsed from JSON.}

\item{max.message.length}{If not NA, shorten commit messages above
this length.}
}
\value{
The commit log as a \code{data.table} with columns source,
  repo, hash, parents, author, author.date.raw, author.time,
  author.tz, committer, commit.date.raw, commit.time, commit.tz,
  message, tag and backend.
}
\description{
Converts a Git log parsed from JSON as a \code{data.table}.
}
