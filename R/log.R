#' Fix Git Log
#'
#' Fix Apache Git logs by filtering out commits with no timezone.
#'
#' @param log The git log.
#' @return The log with commits filtered out.
FixGitLog <- function(log) {
  ## First commit time and first commit time with tz not UTC
  first <- log[, {
    first.tz <- min(time[tz != "+0000" & tz != "UTC"])
    first <- min(time)
    list(ncommits=.N, ncommits2=sum(time < first.tz),
         nauthors=length(unique(author[time < first.tz])),
         diff=as.numeric(difftime(first.tz, first, units="days")),
         first=first, first.tz=first.tz)
  }, by="project"]
  log <- merge(log, first[, list(project, first.tz, first)], by="project")
  log <- log[source != "VCS Apache" | first == first.tz | time >= first.tz]
  log <- log[project != "hive" | time >= as.POSIXct("2015-04-22")]
  log$first <- NULL
  log$first.tz <- NULL
  log
}

#' Find bots
#'
#' Find bots in gitlog based on email address.
#'
#' @param log The git log.
#' @return The log with a boolean bot column.
FindBots <- function(log) {
  bots <- c("bugzilla@gtalbot.org",
            "release-mgmt-account-bot@mozilla.tld", "orangefactor@bots.tld",
            "pulsebot@bots.tld", "tbplbot@gmail.com",
            "release\\+b2gbumper@mozilla.com",
            "release\\+gaiajson@mozilla.com", "release\\+l10nbumper@mozilla.com")
  log[, bot := grepl(paste(bots, collapse="|"), author, ignore.case=TRUE)]
  log
}

## TODO
## Apache: "cvs2svn", "svn-role"
## Need to grep because different ones: jenkins

#' Parse bug id
#'
#' Parses Mozilla bug ids from log messages.
#'
#' @param log.message Log messages.
#' @return Bug ids parsed from log messages.
ParseBugId <- function(log.message) {
  bug.id.re <- "^(.*(bug |bg |b=)#?)?[^[:alnum:]]*(\\d+).*$"
  as.numeric(sub(bug.id.re, "\\3", log.message, ignore.case=TRUE))
}

#' Git log
#'
#' Makes git log with fixes and identity merging.
#'
#' @param git.in Git RDS input.
#' @param bugzilla.in Bugzilla RDS input.
#' @param idmerging.in Identity merging RDS input.
#' @param git.out RDS output file.
#' @export
GitLog <- function(git.in, bugzilla.in, idmerging.in, git.out) {
  log <- readRDS(git.in)
  mozbugs <- readRDS(bugzilla.in)
  setkey(mozbugs, bug.id)

  log <- FixGitLog(log)
  log <- FindBots(log)
  log <- log[!log$bot]

  log[, bug.id := ParseBugId(message)]
  log[source == "VCS Mozilla", origin := project]
  log[grepl("^VCS Mozilla", source),
      project := mozbugs[list(ParseBugId(message)), product]]

  log <- log[, if (.N > 2000) .SD, by="project"]

  log$date <- NULL
  ignore <- "tomcat\\d+"
  log <- log[!grepl(ignore, project)]
  setkey(log, project, source, hash)

  log[, local.time := LocalTime(time, tz)]
  log[, date := as.Date(local.time)]
  log[, week := AutoTimeUtils::ISOWeek(local.time)]
  log[, weekday := format(local.time, "%u")]
  log[, hour := format(local.time, "%H")]
  log[, weekend := weekday > "5"]
  log[, year := year(date)]
  log[, month := as.Date(format(date, "%Y-%m-01"))]

  idmerging <- as.data.table(read.csv(idmerging.in, stringsAsFactors=FALSE))
  idmerging <- idmerging[author.key %in% log$author,
                         list(author=unique(author.key),
                              author.name=names(which.max(table(author.email)))[1],
                              author.email=names(which.max(table(author.name))))[1],
                         by=c("source", "merged.id")]

  log <- merge(idmerging, log, by=c("source", "author"))

  saveRDS(log, git.out)
  invisible(NULL)
}

#' Bugzilla log
#'
#' Makes bugzilla log.
#'
#' @param idmerging Identity merging.
#' @param comments.in Bugzilla comments RDS input.
#' @return Bugzilla log.
BugzillaLog <- function(idmerging, comments.in) {
  bugzilla <- readRDS(comments.in)
  bugzilla <- merge(idmerging[, list(source, merged.id, author.key, author)],
                    bugzilla, by.x=c("source", "author.key"),
                    by.y=c("source", "author.email"))
  bugzilla$author.key <- NULL
  bugzilla[, created := as.POSIXct(created, tz="Z")]
  bugzilla[, updated := as.POSIXct(created, tz="Z")]
  bugzilla
}

#' Jira log
#'
#' Makes Jira log.
#'
#' @param idmerging Identity merging.
#' @param comments.in Jira comments RDS input.
#' @return Jira log.
JiraLog <- function(idmerging, comments.in) {
  jira <- readRDS(comments.in)
  jira <- merge(jira, idmerging[, list(source, merged.id, author.key, author)],
                by.x=c("source", "author.key", "author.dname"),
                by.y=c("source", "author.key", "author"))
  setnames(jira, "author.dname", "author")
  jira$author.key <- NULL
  jira$author.name <- NULL
  jira$author.email <- NULL
  jira <- unique(jira)
  jira[, created := as.POSIXct(created, tz="Z")]
  jira[, updated := as.POSIXct(created, tz="Z")]
  jira[, bug.id := as.integer(bug.id)]
  jira
}

#' Bug log.
#'
#' Makes Jira and Bugzilla bug log.
#'
#' @param idmerging.in Input RDS file of identity merging
#' @param bugzilla.comments.in Input RDS file of Bugzilla comments.
#' @param bugzilla.bugs.in Input RDS file of Bugzilla bugs.
#' @param jira.comments.in Input RDS file of Jira comments.
#' @param jira.bugs.in Input RDS file of Jira bugs.
#' @param buglog.out Bugzilla bugs output RDS file.
#' @export
BugLog <- function(idmerging.in, bugzilla.comments.in, bugzilla.bugs.in,
                   jira.comments.in, jira.bugs.in, buglog.out) {
  idmerging <- read.csv(idmerging.in, stringsAsFactors=FALSE)
  idmerging <- as.data.table(idmerging)
  bugzilla <- BugzillaLog(idmerging, bugzilla.comments.in)
  jira <- JiraLog(idmerging, jira.comments.in)
  bugzilla.bugs <- readRDS(bugzilla.bugs.in)
  jira.bugs <- readRDS(jira.bugs.in)

  bugs <- rbind(bugzilla.bugs[, list(source, bug.id, product, component, status,
                                     resolution, severity)],
                jira.bugs[, list(source, bug.id, product, component, status,
                                 resolution, severity)])
  buglog <- merge(bugs, rbind(jira, bugzilla, fill=TRUE),
                  by=c("source", "bug.id"))
  saveRDS(buglog, buglog.out)
  invisible(NULL)
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

#' Git timezone history
#'
#' Computes timezone history from git log.
#'
#' @param git.in Input gitlog file.
#' @param tz.out Output RDS file.
#' @export
GitTimeZoneHistory <- function(git.in, tz.out) {
  gitlog <- readRDS(git.in)
  gitlog$message <- NULL
  gitlog <- gitlog[, list(size=pmax(sum(added), sum(removed))),
                   by=c("source", "project", "hash", "merged.id", "tz", "time")]
  gitlog[, source := sub("VCS ", "", source)]

  tz.history <- gitlog[order(time),
                       if (.N >= 100) TimeZoneHistory(tz, time),
                       by=c("source", "merged.id")]
  saveRDS(tz.history, tz.out)
  invisible(NULL)
}

#' Filtered bug log
#'
#' Filters bug log to only keep people with timezone from git log..
#'
#' @param buglog.in Buglog RDS input file.
#' @param tz.in Time zone history RDS input file.
#' @param buglog.out Output file.
#' @export
FilteredBugLog <- function(buglog.in, tz.in, buglog.out) {
  buglog <- readRDS(buglog.in)
  buglog[, bug.id := as.character(bug.id)]
  buglog[, comment.id := as.integer(comment.id)]
  tz.history <- readRDS(tz.in)

  buglog <- merge(buglog, unique(tz.history[, list(source, merged.id)]),
                  by=c("source", "merged.id"))

  setkey(tz.history, source, merged.id, time)
  setkey(buglog, source, merged.id, created)
  setnames(buglog, "tz", "tz.orig")

  buglog <- tz.history[buglog, roll=TRUE, rollends=TRUE]

  saveRDS(buglog, buglog.out)
  invisible(NULL)
}

#' Combined log
#'
#' Makes combined Git, Jira and Bugzilla log.
#'
#' @param git.in Git log RDS input file.
#' @param bugs.in Bug log RDS input file.
#' @param pos.in POS metrics RDS input file.
#' @param file.out Output RDS file.
#' @export
CombinedLog <- function(git.in, bugs.in, pos.in, file.out) {
  gitlog <- readRDS(git.in)
  gitlog$message <- NULL
  gitlog <- gitlog[, list(size=pmax(sum(added), sum(removed))),
                   by=c("source", "project", "hash", "merged.id",
                        "author", "bot", "tz", "time")]

  gitlog[, source := sub("VCS ", "", source)]
  setnames(gitlog, "hash", "id")
  gitlog[, type := "VCS commit"]

  buglog <- readRDS(bugs.in)

  metrics <- readRDS(pos.in)[, list(size=sum(nchar)),
                             by=list(source, bug.id, comment.id)]
  buglog[, bug.id := as.integer(bug.id)]
  buglog[, comment.id := as.integer(comment.id)]
  setkey(metrics, source, bug.id, comment.id)
  setkey(buglog, source, bug.id, comment.id)
  buglog <- metrics[buglog]
  buglog[is.na(size), size := 0]

  buglog[, id := paste(bug.id, comment.id)]
  buglog$bug.id <- NULL
  buglog$comment.id <- NULL
  buglog$updated <- NULL
  buglog[, type := "Bug comment"]
  setnames(buglog, "product", "project")

  log <- rbind(gitlog, buglog, fill=TRUE)
  log <- AutoTimeUtils::CreateTimeVariables(log)

  saveRDS(log, file.out)
  invisible(NULL)
}
