#' Parse Commit
#'
#' Converts a commit given as a list to a \code{data.table}
#'
#' @param commit The commit as a list (parsed from JSON).
#' @param max.message.length If not NA, shorten commit messages above
#'   this length.
#' @return A \code{data.table}.
ParseCommit <- function(commit, max.message.length=NA) {
  commit <- c(commit[c("tag", "backend_name")],
              commit$data[c("commit", "Author", "AuthorDate",
                            "Commit", "CommitDate",
                            "parents", "message")])
  commit$parents <- paste(commit$parents, collapse=" ")
  if (!is.na(max.message.length) &&
      nchar(commit$message) >= max.message.length) {
    commit$message <- substr(commit$message, 1, max.message.length)
  }
  as.data.table(commit)
}

#' Parse Diff
#'
#' Converts a diff off a commit to \code{data.table}
#'
#' @param commit The commit as a list (parsed from JSON).
#' @return A \code{data.table} with columns hash, file, added, removed
#'   and action.
ParseDiff <- function(commit) {
  files <- rbindlist(lapply(commit$data$files, function(f) {
    f <- f[c("file", "added", "removed", "action")]
    c(list(hash=commit$data$commit), f[!is.na(names(f))])
  }), fill=TRUE)
}

#' Parse Git log.
#'
#' Converts a Git log parsed from JSON as a \code{data.table}.
#'
#' @param source Source of the repository (e.g. Apache or Mozilla).
#' @param repo Name of the Git repository.
#' @param log List of commits parsed from JSON.
#' @param max.message.length If not NA, shorten commit messages above
#'   this length.
#' @return The commit log as a \code{data.table} with columns source,
#'   repo, hash, parents, author, author.date.raw, author.time,
#'   author.tz, committer, commit.date.raw, commit.time, commit.tz,
#'   message, tag and backend.
ParseGitLog <- function(source, repo, log, max.message.length=NA) {
  log <- rbindlist(lapply(log, ParseCommit, max.message.length), fill=TRUE)
  setnames(log, "commit", "hash", skip_absent=TRUE)
  setnames(log, "Author", "author", skip_absent=TRUE)
  setnames(log, "AuthorDate", "author.date.raw", skip_absent=TRUE)
  setnames(log, "Commit", "committer", skip_absent=TRUE)
  setnames(log, "CommitDate", "commit.date.raw", skip_absent=TRUE)
  log[, author.tz := sub("^.*([-+][0-9]+)$", "\\1", author.date.raw)]
  log[, commit.tz := sub("^.*([-+][0-9]+)$", "\\1", commit.date.raw)]
  log[, author.time := lubridate::parse_date_time(author.date.raw, "ab!d!H!M!S!Y!z!")]
  log[, commit.time := lubridate::parse_date_time(commit.date.raw, "ab!d!H!M!S!Y!z!")]
  tz(log$author.time) <- ""
  tz(log$commit.time) <- ""
  log[, list(source, repo, hash, parents, author, author.date.raw,
             author.time, author.tz, committer, commit.date.raw,
             commit.time, commit.tz, message, tag,
             backend=backend_name)]
}

#' Parse Git Repository
#'
#' Parses a git repository JSON output as a log and diff.
#'
#' @param source Source of the repository (e.g. Apache or Mozilla).
#' @param repo Name of the Git repository.
#' @param file.in Input JSON file.
#' @param log.out Output Parquet file for the Git log.
#' @param diff.out Output Parquet file for the diff.
#' @param max.message.length If not NA, shorten commit messages above
#'   this length.
#' @return If output files are not NULL, returns the file names inside
#'   a \code{data.table} with columns source, repo, log and
#'   diff. Otherwise returns the \code{data.table} log and diff inside
#'   a list.
#' @export
ParseGitRepository <- function(source, repo, file.in,
                               log.out=NULL, diff.out=NULL,
                               max.message.length=NA) {
  logging::loginfo("Reading git log %s", file.in)
  log <- jsonlite::read_json(file.in)
  if (length(log)) {
    diff <- rbindlist(lapply(log, ParseDiff), fill=TRUE)
    diff <- diff[, list(source, repo, hash, file,
                        file.extension=ParseExtension(file),
                        action, added, removed)]
    log <- ParseGitLog(source, repo, log, max.message.length)
  } else {
    diff <- data.table(source, repo)
    log <- data.table(source, repo)
  }
  if (!is.null(log.out) && !is.null(diff.out)) {
    WriteParquet(log, log.out)
    WriteParquet(diff, diff.out)
    data.table(source, repo, log=log.out, diff=diff.out)
  } else list(log=log, diff=diff)
}

#' Simplify Git Log
#'
#' Simplifies a git log and mergres it with diff.
#'
#' @param files List with the names of the Parquet files for the Git
#'   log and diff.
#' @param by.file.extension If TRUE, number of added and removed lines
#'   of code are summed by file extensions, otherwise summed by
#'   commit.
#' @return The commit log as a \code{data.table} with columns source,
#'   repo, hash, parents, author, author.time, author.tz, committer,
#'   commit.time, commit.tz, message, added and removed (and
#'   file.extension if \code{by.file.extension} is TRUE).
#' @export
SimplifyGitlog <- function(files, by.file.extension=FALSE) {
  log <- ReadParquet(files$log)
  log[grep("_github$", source), source := gsub("_github$", "", source)]
  diff <- ReadParquet(files$diff)
  diff[grep("_github$", source), source := gsub("_github$", "", source)]
  if (ncol(log) > 2) {
    log <- RemoveColumns(log, c("author.date.raw", "commit.date.raw",
                                "tag", "backend"))
    diff[, added := as.numeric(added)]
    diff[, removed := as.numeric(removed)]
    group.by <- c("source", "repo", "hash")
    if (by.file.extension) groupby <- c(groupby, "file.extension")
    diff <- diff[, list(added=sum(added, na.rm=TRUE),
                        removed=sum(removed, na.rm=TRUE)),
                 by=eval(group.by)]
    merge(log, diff, by=c("source", "repo", "hash"), all=TRUE)
  }
}
