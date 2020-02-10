#' First Time
#'
#' Returns first timestamp with valid Git timezone (first timestamp
#' from 2007 and beyond that is not UTC timezone).
#'
#' @param time Vector of timestamps.
#' @param tz Vector of timezones.
#' @return A POSIXct object.
FirstTime <- function(time, tz) {
  min(time[tz != "+0000" & year(time) >= 2007])
}

#' Commit TimeZone Accuracy
#'
#' Adds boolean columns (from.svn and accurate.tz) to a data.table of
#' commits for determining which commits have accurate timezones.
#'
#' @param log Commit log as a data.table object.
#' @return The modified commit log with added from.svn and accurate.tz
#'   columns.
CommitTimeZoneAccuracy <- function(log) {
  logging::loginfo("Computing commit timezone accuracy")
  gitsvn.re <- "git-svn-id: https?://[^ ]+ [-a-f0-9]+(\n\nFormer-commit-id: [-a-f0-9]+)?$"
  log[, from.svn := grepl(gitsvn.re, message)]
  log[, accurate.tz := (!from.svn &
                        author.time >= FirstTime(author.time, author.tz) &
                        commit.time >= FirstTime(commit.time, commit.tz)),
      by=list(source, repo)]
}

#' Parse Mozilla Issue Id
#'
#' Parses issue ids from Mozilla commit log messages.
#'
#' @param log.message Vector of commit log messages.
#' @return Vecotr of issue id integers (with possible NAs).
ParseMozillaIssueId <- function(log.message) {
  logging::loginfo("Parsing Mozilla issue ids from commit messages.")
  issue.id.re <- "^(.*(bug |bg |b=)#?)?[^[:alnum:]]*(\\d+).*$"
  as.numeric(sub(issue.id.re, "\\3", log.message, ignore.case=TRUE))
}

#' Parse Apache Issue Key
#'
#' Parses issue keys from Apache commit log messages.
#'
#' @param log.message Vector of commit log messages.
#' @return A vector where each element is a string of issue keys
#'   separated by spaces.
ParseApacheIssueKey <- function(log.message) {
  logging::loginfo("Parsing Apache issue keys from commit messages.")
  issue.id.re <- "^(([^-[:alnum:]]*[A-Z0-9]+-[0-9]+[^-[:alnum:]])+).*$"
  res <- sub(issue.id.re, "\\1", log.message)
  res[!grepl(issue.id.re, log.message)] <- NA
  sapply(strsplit(res, "[^-A-Z0-9]"), function(k) {
    if (!is.na(k)) {
      paste(k[k != ""], collapse=" ")
    } else k
  })
}

#' Commit Log
#'
#' Processes raw git log by determinig time zone accuracy and linking
#' commits to issues.
#'
#' @param git.input Parquet file containing the raw git log.
#' @param issues.input Parquet file containing the raw issue log.
#' @param output Parquet file where to store the result.
#' @return The output filename (if any) or the data.table object
#'   otherwise.
#' @export
CommitLog <- function(git.input, issues.input, output=NULL) {
  log <- ReadParquet(git.input)
  log <- CommitTimeZoneAccuracy(log)
  log[source == "mozilla", issue.id := ParseMozillaIssueId(message)]
  log[source == "apache", issue.key := ParseApacheIssueKey(message)]
  log <- log[, if (!is.na(issue.key)) {
                 cbind(.SD[, -grep("issue.key", names(.SD)), with=FALSE],
                       issue.key=strsplit(issue.key, " ")[[1]])
               } else .SD,
             by=list(source, repo, hash)]
  sub <- with(log, is.na(issue.id) & !is.na(issue.key))
  keys <- log[sub, list(source, issue.key)]
  issues <- ReadParquet(issues.input)
  log[sub]$issue.id <- issues[keys, issue.id, on=list(source, issue.key)]
  log$issue.key <- NULL
  if (!is.null(output)) {
    WriteParquet(log, output)
    output
  } else log
}

#' Time zone history
#'
#' Computes time zone history from list of timestamps and timezones
#'
#' @param tz Timezones.
#' @param times Timestamps.
#' @return \code{data.table} object of list of times when timezone changed.
TimeZoneHistory <- function(tz, times) {
  changes <- c(TRUE, shift(tz)[-1] != tz[-1])
  list(time=times[changes], tz=tz[changes])
}

#' Commit Time Zone History
#'
#' Computes timezone history in commits for developer profiles based
#' on identity merging.
#'
#' @param log.input Parquet file containing the commit log.
#' @param idmerging.input Parquet file with identity merging.
#' @param output Parquet file where to store the output.
#' @return The output filename (if any) or the data.table object
#'   otherwise.
#' @export
CommitTimeZoneHistory <- function(log.input, idmerging.input, output) {
  log <- ReadParquet(log.input)[(accurate.tz)]
  idmerging <- ReadParquet(idmerging.input)
  idmerging <- unique(idmerging[type == "commits",
                                list(source, repo, key, merged.id)])
  tz.history <- rbind(log[, list(source, repo, key=author,
                                 tz=author.tz, time=author.time)],
                      log[, list(source, repo, key=committer,
                                 tz=commit.tz, time=commit.time)])
  tz.history <- merge(idmerging, unique(tz.history),
                      by=c("source", "repo", "key"))
  tz.history <- tz.history[order(source, merged.id, time)]
  tz.history <- tz.history[, TimeZoneHistory(tz, time),
                           by=c("source", "merged.id")]
  if (!is.null(output)) {
    WriteParquet(tz.history, output)
    output
  } else tz.history
}

#' Commit Timestamps
#'
#' Returns list of timestamps for commits.
#'
#' @param commits.input parquet file containing the commit log.
#' @param idmerging Identity merging data.table.
#' @return The commit timestamp log.
CommitTimestamps <- function(commits.input, idmerging) {
  idmerging <- unique(idmerging[!is.na(repo),
                                list(source, repo, key, merged.id)])
  commits <- (ReadParquet(commits.input) %>%
              setnames("author", "author.key") %>%
              merge(idmerging,
                    by.x=c("source", "repo", "author.key"),
                    by.y=c("source", "repo", "key"), all.x=TRUE) %>%
              setnames("merged.id", "author") %>%
              setnames("committer", "committer.key") %>%
              merge(idmerging,
                    by.x=c("source", "repo", "committer.key"),
                    by.y=c("source", "repo", "key"), all.x=TRUE) %>%
              setnames("merged.id", "committer"))
  res <- rbind(commits[!is.na(author),
                       list(source, repo, hash,
                            time=author.time, tz=author.tz, accurate.tz,
                            person=author, type="commit", action="authored")],
               commits[!is.na(committer),
                       list(source, repo, hash,
                            time=commit.time, tz=commit.tz, accurate.tz,
                            person=committer, type="commit", action="committed")])
  res[(!accurate.tz), tz := NA]
  res$accurate.tz <- NULL
  res
}

#' Issue Timestamps
#'
#' Returns list of timestamps for issues.
#'
#' @param issues.input parquet file containing the issue log.
#' @param idmerging Identity merging data.table.
#' @return The issue timestamp log.
IssueTimestamps <- function(issues.input, idmerging) {
  idmerging <- unique(idmerging[is.na(repo), list(source, key, merged.id)])
  issues <- (ReadParquet(issues.input) %>%
             merge(idmerging,
                   by.x=c("source", "reporter.key"),
                   by.y=c("source", "key"), all.x=TRUE) %>%
             setnames("merged.id", "reporter") %>%
             merge(idmerging,
                   by.x=c("source", "creator.key"),
                   by.y=c("source", "key"), all.x=TRUE) %>%
             setnames("merged.id", "creator"))
  rbind(issues[!is.na(creator),
               list(source, product, issue.id,
                    time=created, person=creator,
                    type="issue", action="created")],
        issues[!is.na(reporter),
               list(source, product, issue.id,
                    time=created, person=reporter,
                    type="issue", action="reported")],
        issues[!is.na(creator) & created < updated,
               list(source, product, issue.id,
                    time=updated, person=creator,
                    type="issue", action="updated")])
}

#' Comment Timestamps
#'
#' Returns list of timestamps for issue comments.
#'
#' @param comments.input parquet file containing the issue comment log.
#' @param idmerging Identity merging data.table.
#' @return The issue comment timestamp log.
CommentTimestamps <- function(comments.input, idmerging) {
  idmerging <- unique(idmerging[is.na(repo), list(source, key, merged.id)])
  comments <- (ReadParquet(comments.input) %>%
               merge(idmerging,
                     by.x=c("source", "author.key"),
                     by.y=c("source", "key"), all.x=TRUE) %>%
               setnames("merged.id", "author") %>%
               merge(idmerging,
                     by.x=c("source", "update.author.key"),
                     by.y=c("source", "key"), all.x=TRUE) %>%
               setnames("merged.id", "update.author"))
  rbind(comments[!is.na(author),
                 list(source, product, issue.id, comment.id,
                      time=created, person=author,
                      type="comment", action="created")],
        comments[!is.na(author) & is.na(update.author) &
                 created < updated,
                 list(source, product, issue.id, comment.id,
                      time=updated, person=author,
                      type="comment", action="updated")],
        comments[!is.na(update.author) & created < updated,
                 list(source, product, issue.id, comment.id,
                      time=updated, person=update.author,
                      type="comment", action="updated")])
}

#' Timestamps
#'
#' Returns list of timestamps for commits, issues and issue comments.
#'
#' @param commits.input parquet file containing the commit log.
#' @param issues.input parquet file containing the issue log.
#' @param comments.input parquet file containing the issue comment
#'   log.
#' @param idmerging.input Parquet file containing identity merging.
#' @param tzhistory.input Parquet file containing the commit timezone
#'   history.
#' @param output Parquet file where to store the output.
#' @return The output filename (if any) or the data.table object
#'   containing the timestamp log otherwise.
#' @export
Timestamps <- function(commits.input, issues.input, comments.input,
                       idmerging.input, tzhistory.input, output=NULL) {
  idmerging <- ReadParquet(idmerging.input)
  issues.ts <- rbind(IssueTimestamps(issues.input, idmerging),
                     CommentTimestamps(comments.input, idmerging),
                     fill=TRUE)
  tzhistory <- ReadParquet(tzhistory.input)
  setnames(tzhistory, "merged.id", "person")
  res <- rbind(CommitTimestamps(commits.input, idmerging),
               tzhistory[issues.ts, roll=TRUE, rollends=TRUE,
                         on=c("source", "person", "time")], fill=TRUE)
  if (!is.null(output)) {
    WriteParquet(res, output)
    output
  } else res
}

## ## TODO
## ## Apache: "cvs2svn", "svn-role"
## ## Need to grep because different ones: jenkins
