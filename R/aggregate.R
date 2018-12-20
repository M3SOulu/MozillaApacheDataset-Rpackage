#' Aggregate Git
#'
#' Aggregate Git raw data files.
#'
#' @param log.in List of git log input files (as a gzipped CSV).
#' @param diff.in List of git diff input files (as a gzipped CSV).
#' @param file.out Output RDS file.
#' @export
AggregateGit <- function(log.in, diff.in, file.out)  {
  ## Reading both Git logs and diffs. Only keep diffs with numeric
  ## added/removed lines (i.e. get rid of binary files). Aggregate diff
  ## by file types.
  log <- rbindlist(mapply(function(f.log, f.diff) {
    logging::loginfo("Reading git log %s", f.log)
    log <- ReadFile(f.log)
    diff <- ReadFile(f.diff)
    diff[, added := as.numeric(added)]
    diff[, removed := as.numeric(removed)]
    diff <- diff[!is.na(added), list(added=sum(added), removed=sum(removed)),
                 by=list(project, hash, extension=ParseExtension(file))]
    setkey(diff, project, hash)
    setkey(log, project, hash)
    merge(diff, log, by=c("project", "hash"))
  }, log.in, diff.in, SIMPLIFY=FALSE))

  ## Add sources (Apache, Taliso, Mozilla)
  log[, source := "VCS Apache"]
  log[project == "harja", source := "VCS Taliso"]
  log[project %in% c("mozilla-unified", "comm-central", "mobile-browser"),
      source := "VCS Mozilla"]

  ## Reads timezone and parse author times
  log[, tz := sub("^.*([-+][0-9]+)$", "\\1", date)]
  log[, time := lubridate::parse_date_time(date, "ab!d!H!M!S!Y!z!")]

  saveRDS(log, file.out)
  invisible(NULL)
}

#' Aggregate Jira bugs
#'
#' Aggregate Jira raw bugs.
#'
#' @param bugs.in List of bugs input files (as a gzipped CSV).
#' @param components.in List of components input files (as a gzipped CSV).
#' @param file.out Output RDS file.
#' @export
AggregateJiraBugs <- function(bugs.in, components.in, file.out) {
  bugs <- rbindlist(mapply(function(f.bugs, f.comp) {
    logging::loginfo("Reading jira bugs for %s", f.bugs)
    bugs <- ReadFile(f.bugs, FALSE)
    if (file.exists(f.comp)) {
      components <- ReadFile(f.comp)
      setnames(components, "name", "component")
      if ("description" %in% names(components)) {
        setnames(components, "description", "component.description")
      }
      setkey(components, project, product.key, bug.id)
      setkey(bugs, project, product.key, bug.id)
      components[bugs]
    } else bugs
  }, bugs.in, components.in, SIMPLIFY=FALSE), fill=TRUE)

  bugs <- bugs[, list(source=project, bug.id, product, component,
                      component.description, status,
                      resolution=resolution.name,
                      resolution.description, severity=priority,
                      created, summary, description)]
  saveRDS(bugs, file.out)
  invisible(NULL)
}

#' Aggregate Jira versions
#'
#' Aggregate Jira raw versions.
#'
#' @param files.in List of input files (as a gzipped CSV).
#' @param file.out Output RDS file.
#' @export
AggregateJiraVersions <- function(files.in, file.out) {
  versions <- rbindlist(lapply(files.in, function(f) {
    logging::loginfo("Reading jira versions for %s", f)
    ReadFile(f)
  }), fill=TRUE)
  setnames(versions, "releaseDate", "release.date")

  saveRDS(versions, file.out)
  invisible(NULL)
}

#' Aggregate Jira comments
#'
#' Aggregate Jira raw comments.
#'
#' @param files.in List of input files (as a gzipped CSV).
#' @param file.out Output RDS file.
#' @export
AggregateJiraComments <- function(files.in, file.out) {
  comments <- rbindlist(lapply(files.in, function(f) {
    logging::loginfo("Reading jira comments for %s", f)
    comments <- ReadFile(f)
    comments[, list(source=project, bug.id, comment.id=1:.N, created, updated,
                    author.name, author.email=author.emailAddress,
                    author.dname=author.displayName, author.key,
                    tz=author.timeZone)]
  }))

  comments[, created := format(as.POSIXct(created, "%FT%T", tz="UTC"),
                               "%F %T", tz="UTC")]
  comments[, updated := format(as.POSIXct(updated, "%FT%T", tz="UTC"),
                               "%F %T", tz="UTC")]

  saveRDS(comments, file.out)
  invisible(NULL)
}

#' Aggregate Bugzilla bugs
#'
#' Aggregate Bugzilla raw bugs.
#'
#' @param files.in List of input files (as a gzipped CSV).
#' @param file.out Output RDS file.
#' @export
AggregateBugzillaBugs <- function(files.in, file.out) {
  bugs <- rbindlist(lapply(files.in, function(f) {
    logging::loginfo("Reading Mozilla bugs for %s", f)
    ReadFile(f)
  }), fill=TRUE)
  bugs <- bugs[, list(source="Mozilla", bug.id, product, component, status,
                      resolution, severity, summary)]
  saveRDS(bugs, file.out)
  invisible(NULL)
}

#' Aggregate Bugzilla comments
#'
#' Aggregate Bugzilla raw comments.
#'
#' @param files.in List of input files (as a gzipped CSV).
#' @param file.out Output RDS file.
#' @export
AggregateBugzillaComments <- function(files.in, file.out) {
  comments <- rbindlist(lapply(files.in, function(f) {
    logging::loginfo("Reading Mozilla comments for %s", f)
    comments <- ReadFile(f, FALSE)
    comments[, list(source=project, bug.id=id, comment.id=1:.N,
                    created=creation_time, updated=time,
                    author.email=author, attachment=attachment_id)]
  }))

  comments[, created := format(as.POSIXct(created, "%FT%T", tz="UTC"),
                               "%F %T", tz="UTC")]
  comments[, updated := format(as.POSIXct(updated, "%FT%T", tz="UTC"),
                               "%F %T", tz="UTC")]
  saveRDS(comments, file.out)
  invisible(NULL)
}
