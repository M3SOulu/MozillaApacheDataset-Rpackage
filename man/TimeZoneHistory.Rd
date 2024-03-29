% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/log.R
\name{TimeZoneHistory}
\alias{TimeZoneHistory}
\title{Time zone history}
\usage{
TimeZoneHistory(tz, times)
}
\arguments{
\item{tz}{Timezones.}

\item{times}{Timestamps.}
}
\value{
\code{data.table} object of list of times when timezone changed.
}
\description{
Computes time zone history from list of timestamps and timezones
}
