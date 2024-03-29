% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/nlp.R
\name{ProcessComments}
\alias{ProcessComments}
\title{Process Comments}
\usage{
ProcessComments(FUNC, file.in, file.out = NULL, ...)
}
\arguments{
\item{FUNC}{Function to apply.}

\item{file.in}{Input Parquet filename.}

\item{file.out}{Output Parquet filename.}

\item{...}{Additional parameters to pass to \code{FUNC}.}
}
\value{
The result of \code{FUNC} applied to the comments if
  \code{file.out} is NULL, or the output filename otherwise.
}
\description{
Processes comments with a function.
}
