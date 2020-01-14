ParseCommit <- function(commit) {
  files <- rbindlist(lapply(commit$data$files, function(f) {
    f <- f[c("file", "added", "removed", "action")]
    f[!is.na(names(f))]
  }), fill=TRUE)
  commit <- c(commit[c("tag", "backend_name")],
              commit$data[c("commit", "Author", "AuthorDate",
                            "Commit", "CommitDate",
                            "parents", "message")],
              as.list(files))
  commit$parents <- paste(commit$parents, collapse=" ")
  as.data.table(commit)
}

ParseGitRepository <- function(source, repo, file.in, file.out=NULL) {
  logging::loginfo("Reading git log %s", file.in)
  log <- jsonlite::read_json(file.in)
  log <- rbindlist(lapply(log, ParseCommit), fill=TRUE)
  setnames(log, "commit", "hash", skip_absent=TRUE)
  setnames(log, "Author", "author", skip_absent=TRUE)
  setnames(log, "AuthorDate", "author.date.raw", skip_absent=TRUE)
  setnames(log, "Commit", "committer", skip_absent=TRUE)
  setnames(log, "CommitDate", "commit.date.raw", skip_absent=TRUE)
  log[, author.tz := sub("^.*([-+][0-9]+)$", "\\1", author.date.raw)]
  log[, commit.tz := sub("^.*([-+][0-9]+)$", "\\1", commit.date.raw)]
  log[, author.time := lubridate::parse_date_time(author.date.raw, "ab!d!H!M!S!Y!z!")]
  log[, commit.time := lubridate::parse_date_time(author.date.raw, "ab!d!H!M!S!Y!z!")]
  tz(log$author.time) <- ""
  tz(log$commit.time) <- ""
  log[, file.extension := ParseExtension(file)]
  log <- log[, list(source, repo, hash, parents,
                    author, author.date.raw, author.time, author.tz,
                    committer, commit.date.raw, commit.time, commit.tz,
                    message, file, file.extension, action,
                    added, removed, tag, backend=backend_name)]
  if (!is.null(file.out)) {
    WriteParquet(log, file.out)
    file.out
  } else log
}

AggregateGit <- function(input.files, file.out=NULL, groupby="hash") {
  if (groupby == "extension") groupby <- "file.extension"
  log <- rbindlist(lapply(input.files, function(f) {
    logging::loginfo("Reading git log %s", f)
    l <- ReadParquet(f)
    if (groupby %in% c("hash", "file.extension")) {
      l$file <- NULL
      l$action <- NULL
      l[, added := as.numeric(added)]
      l[, removed := as.numeric(removed)]
      diff <- l[, list(hash, file.extension, added, removed, tag, backend)]
      if (!"hash" %in% groupby) groupby <- c("hash", groupby)
      diff <- diff[, list(added=sum(added, na.rm=TRUE),
                          removed=sum(removed, na.rm=TRUE),
                          tag=tag[1], backend=backend[1]),
                   by=eval(groupby)]
      l$file.extension <- NULL
      l$added <- NULL
      l$removed <- NULL
      l$tag <- NULL
      l$backend <- NULL
      merge(l[, .SD[1], by=list(source, repo, hash)], diff, by="hash")
    } else {
      ReadParquet(f)
    }
  }))
  if (!is.null(file.out)) {
    WriteParquet(log, file.out)
    file.out
  } else log
}
