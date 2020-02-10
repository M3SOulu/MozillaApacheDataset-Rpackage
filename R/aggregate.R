#' Aggregate
#'
#' Aggregate several Parquet tables as a single one.
#'
#' @param input.files List of Parquet files.
#' @param FUNC Function to apply on each Parquet table.
#' @param cols Columns to subset.
#' @param file.out Output Parquet filename.
#' @return The aggregated Parquet tables as a \code{data.table} if
#'   \code{file.out} is not NULL, otherwise \code{file.out}.
#' @export
Aggregate <- function(input.files, FUNC, cols=NULL, file.out=NULL) {
  log <- rbindlist(lapply(input.files, FUNC), fill=TRUE)
  if (!is.null(cols)) {
    log <- log %>% SubsetColumns(cols)
  }
  if (!is.null(file.out)) {
    WriteParquet(log, file.out)
    file.out
  } else log
}

#' Aggregate commits
#'
#' Aggregate commits for different repositories as a single table.
#'
#' @param gitlog Data.frame containing for each Git repository the
#'   Parquet filename of the commit log and the Parquet filename of
#'   the diff.
#' @param FUNC Function to apply on each commit table.
#' @param file.out Output Parquet filename.
#' @return The aggregated Parquet tables as a \code{data.table} if
#'   \code{file.out} is not NULL, otherwise \code{file.out}.
#' @export
AggregateCommits <- function(gitlog, FUNC, file.out=NULL) {
  files <- with(gitlog, mapply(function(x, y) list(log=x, diff=y),
                               log, diff, SIMPLIFY=FALSE))
  Aggregate(files, FUNC, file.out=file.out)
}

issues.cols <- list(issues=c("source", "product", "issue.id", "issue.key",
                             "created", "updated", "last.resolved",
                             "summary", "description", "version",
                             "milestone", "status", "severity",
                             "priority", "issuetype", "resolution",
                             "component", "votes", "product.name",
                             "reporter.key", "reporter.name",
                             "reporter.displayname", "reporter.email",
                             "reporter.tz", "creator.key",
                             "creator.name", "creator.displayname",
                             "creator.email", "creator.tz",
                             "assignee.key", "assignee.name",
                             "assignee.displayname", "assignee.email",
                             "assignee.tz"),
                    comments=c("source", "product",
                               "issue.id", "comment.id", "count",
                               "author.key", "author.name",
                               "author.displayname", "author.email",
                               "author.tz", "creator.email",
                               "update.author.key",
                               "update.author.name",
                               "update.author.displayname",
                               "update.author.email",
                               "update.author.tz", "created", "updated"))

#' Aggregate issues
#'
#' Aggregate issue data as a single table.
#'
#' @param jiralog Data.frame containing all the Jira Parquet
#'   filenames.
#' @param bugzillalog Data.frame containing all the Bugzilla Parquet
#'   filenames
#' @param t Type of issue log to create (e.g. issue or comment).
#' @param FUNC Function to apply on each table.
#' @param file.out Output Parquet filename.
#' @return The aggregated Parquet tables as a \code{data.table} if
#'   \code{file.out} is not NULL, otherwise \code{file.out}.
#' @export
AggregateIssues <- function(jiralog, bugzillalog, t,
                            FUNC, file.out=NULL) {
  files <- rbind(jiralog, bugzillalog)[type == t, filename]
  log <- Aggregate(files, function(f) {
    res <- f %>% ReadParquet
    if (nrow(res)) res %>% FUNC
  }, cols=issues.cols[[t]], file.out=file.out)
}
