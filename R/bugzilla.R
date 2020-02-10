bugzilla.columns <- list(issues=c("source", "product", "issue.id",
                                  "created.raw", "created",
                                  "updated.raw", "updated",
                                  "last.resolved.raw",
                                  "last.resolved", "component",
                                  "summary", "version", "milestone",
                                  "status", "severity", "priority",
                                  "resolution", "votes",
                                  "creator.key", "creator.name",
                                  "creator.displayname",
                                  "creator.email", "assignee.key",
                                  "assignee.name",
                                  "assignee.displayname",
                                  "assignee.email", "tag", "backend"),
                         comments=c("source", "product", "issue.id",
                                    "comment.id", "count", "author.key",
                                    "author.email", "creator.email",
                                    "created.raw", "created",
                                    "updated.raw", "updated",
                                    "raw.text", "text"))

#' Bugzilla Time
#'
#' Parses Bugzilla raw time fields as POSIXct.
#'
#' @param table deta.frame containing raw time fields
#' @param name Name of the column in which to add the parsed time.
#' @return The table with the time parsed as POSIXct added as a
#'   column.
BugzillaTime <- function(table, name) {
  raw.name <- sprintf("%s.raw", name)
  if (sprintf("%s.raw", name) %in% names(table)) {
    table[[name]] <- as.POSIXct(table[[raw.name]], "%FT%T", tz="Z")
  }
  table
}

#' Bugzilla User
#'
#' Parses Bugzilla user.
#'
#' @param person A list representing a person.
#' @param type Type of person (e.g. author). This is added as a prefix
#'   of the elements names.
#' @return The person with proper names.
BugzillaUser <- function(person, type="") {
  if (!is.null(person)) {
    cols <- c("id", "name", "real_name", "email")
    person <- person[cols]
    names(person) <- c("key", "name", "displayname", "email")
    person$key <- as.character(person$key)
    if (type != "") {
      names(person) <- paste(type, names(person), sep=".")
    }
    person
  }
}

#' Bugzilla Issue
#'
#' Parses a Bugzilla Issue
#'
#' @param issue Issue as a list (parsed from JSON).
#' @return List of fields.
BugzillaIssue <- function(issue) {
  c(issue$data[c("component", "status", "resolution", "severity",
                 "priority", "version", "votes",
                 "summary")],
    list(issue.id=issue$data$id,
         last.resolved.raw=issue$data$cf_last_resolved,
         milestone=issue$data$target_milestone,
         created.raw=issue$data$creation_time,
         updated.raw=issue$data$last_change_time,
         tag=issue$tag, backend=issue$backend_name),
    BugzillaUser(issue$data$creator_detail, "creator"),
    BugzillaUser(issue$data$assigned_to_detail, "assignee")) %>% RemoveNANames
}

#' Bugzilla History Items
#'
#' Parses a list of Bugzilla history changes
#'
#' @param items List of history changes (parsed from JSON).
#' @return A \code{data.table} representing the changes.
BugzillaHistoryItems <- function(items) {
  lapply(items, function(item) {
    items <- item[c("field_name", "added", "removed")]
    names(items) <- c("field", "from", "to")
    items
  }) %>% rbindlist(fill=TRUE)
}

#' Bugzilla History
#'
#' Parses history from an issue.
#'
#' @param issue List representing the issue (parsed from JSON).
#' @return A \code{data.table} representing the history of change of the issue.
BugzillaHistory <- function(issue) {
  history <- issue$data$history
  if (length(history)) {
    lapply(history, function(h) {
      c(BugzillaHistoryItems(h$changes),
        list(time=h$when, email=h$who))
    }) %>% rbindlist(fill=TRUE) %>% cbind(issue.id=issue$data$id)
  }
}

#' Bugzilla Comment
#'
#' Parses a Bugzilla issue comment.
#'
#' @param comment List representing the comment (parsed from JSON).
#' @return A list representing the comment.
BugzillaComment <- function(comment) {
  with(comment, list(comment.id=id,
                     count=count,
                     author.key=author,
                     author.email=author,
                     creator.key=creator,
                     creator.email=creator,
                     created.raw=creation_time,
                     updated.raw=time,
                     text=text,
                     raw.text=raw_text) %>% RemoveNANames)
}

#' Bugzilla Comments
#'
#' Parses several issue comments.
#'
#' @param issue List representing the issue (parsed from JSON).
#' @return A \code{data.table} representing the issue comments.
BugzillaComments <- function(issue) {
  comments <- issue$data$comments
  if (length(comments)) {
    comments <- lapply(comments, BugzillaComment) %>% rbindlist(fill=TRUE)
    cbind(comments, issue.id=issue$data$id)
  }
}

#' Parse Bugzilla
#'
#' Parse a JSON log representing a set of Bugzilla issues.
#'
#' @param source Source of the issue tracker (Mozilla).
#' @param product Product tag of the issues.
#' @param file.in JSON file (possibly gzipped) containing the issues.
#' @param files.out List of output Parquet files for the issues,
#'   comments and history.
#' @export
ParseBugzilla <- function(source, product, file.in, files.out=NULL) {
  logging::loginfo("Reading Bugzilla log %s", file.in)
  log <- jsonlite::read_json(file.in)
  res <- list(issues=BugzillaIssue, history=BugzillaHistory,
              comments=BugzillaComments)
  res <- mapply(function(type, FUNC) {
    logging::loginfo("Extracting %s", type)
    log %>% lapply(FUNC) %>% rbindlist(fill=TRUE) %>% cbind(source, product)
  }, names(res), res, SIMPLIFY=FALSE)

  res$issues <- (res$issues %>% BugzillaTime("created") %>%
                 BugzillaTime("updated") %>%
                 BugzillaTime("last.resolved") %>%
                 SubsetColumns(bugzilla.columns$issues))
  res$comments <- (res$comments %>%
                   BugzillaTime("created") %>%
                   BugzillaTime("updated") %>%
                   SubsetColumns(bugzilla.columns$comments))

  if (!is.null(files.out) && all(names(res) %in% names(files.out))) {
    for (type in names(res)) {
      WriteParquet(res[[type]], files.out[[type]])
    }
    data.table(source, product, type=names(res), filename=files.out[names(res)])
  } else res
}
