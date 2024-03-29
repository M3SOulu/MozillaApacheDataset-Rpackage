% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/jira.R
\name{JiraUser}
\alias{JiraUser}
\title{Jira User}
\usage{
JiraUser(person, type = "")
}
\arguments{
\item{person}{A list representing a person.}

\item{type}{Type of person (e.g. author). This is added as a prefix
of the elements names.}
}
\value{
The person with proper names.
}
\description{
Parses Jira user.
}
