jira.columns <- list(issues=c("source", "product", "issue.id", "issue.key",
                              "created.raw", "created", "updated.raw",
                              "updated", "summary", "description",
                              "status", "priority", "issuetype",
                              "resolution", "component", "votes",
                              "product.name", "reporter.key",
                              "reporter.name", "reporter.displayname",
                              "reporter.email", "reporter.tz",
                              "creator.key", "creator.name",
                              "creator.displayname", "creator.email",
                              "creator.tz", "assignee.key",
                              "assignee.name", "assignee.displayname",
                              "assignee.email", "assignee.tz", "tag",
                              "backend"),
                     comments=c("source", "product", "issue.id",
                                "comment.id", "author.key",
                                "author.name", "author.displayname",
                                "author.email", "author.tz",
                                "update.author.key",
                                "update.author.name",
                                "update.author.displayname",
                                "update.author.email",
                                "update.author.tz", "created.raw",
                                "created", "updated.raw", "updated",
                                "text"))
#' Jira Time
#'
#' Parses Jira raw time fields as POSIXct.
#'
#' @param table deta.frame containing raw time fields
#' @param name Name of the column in which to add the parsed time.
#' @return The table with the time parsed as POSIXct added as a
#'   column.
JiraTime <- function(table, name) {
  raw.name <- sprintf("%s.raw", name)
  if (sprintf("%s.raw", name) %in% names(table)) {
    table[[name]] <- as.POSIXct(table[[raw.name]], "%FT%T", tz="+0000")
  }
  table
}

#' Jira Versions
#'
#' Gets versions from a Jira issue.
#'
#' @param issue Issue as a list parsed from JSON.
#' @return A \code{data.table} with releases associated with the
#'   issue.
JiraVersions <- function(issue){
  if (length(issue$data$fields$versions)) {
    cbind(rbindlist(lapply(issue$data$fields$versions, function(v) {
      RemoveNANames(v[c("releaseDate", "released", "name", "archived")])
    }), fill=TRUE), issue.id=as.integer(issue$data$id))
  }
}

#' Jira Components
#'
#' Gets components from a Jira issue.
#'
#' @param issue Issue as a list parsed from JSON.
#' @return A character vector with each component on a separate line.
JiraComponents <- function(issue) {
  components <- sapply(issue$data$fields$components, function(c) c$name)
  paste(components, collapse="\n")
}

#' Jira History Items
#'
#' Parses a list of Jira history changes
#'
#' @param items List of history changes (parsed from JSON).
#' @return A \code{data.table} representing the changes.
JiraHistoryItems <- function(items) {
  lapply(items, function(item) {
    items <- item[c("field", "fieldtype", "fromString", "toString")]
    names(items) <- c("field", "fieldtype", "from", "to")
    items
  }) %>% rbindlist(fill=TRUE)
}

#' Jira History
#'
#' Parses history from an issue.
#'
#' @param issue List representing the issue (parsed from JSON).
#' @return A \code{data.table} representing the history of change of the issue.
JiraHistory <- function(issue) {
  history <- issue$data$changelog$histories
  if (length(history)) {
    lapply(history, function(h) {
      c(JiraHistoryItems(h$items), list(time=h$created), JiraUser(h$author))
    }) %>% rbindlist(fill=TRUE) %>% cbind(issue.id=as.integer(issue$data$id))
  }
}

#' Jira Comment
#'
#' Parses a Jira issue comment.
#'
#' @param comment List representing the comment (parsed from JSON).
#' @return A list representing the comment.
JiraComment <- function(comment) {
  c(list(comment.id=as.integer(comment$id),
         text=comment$body,
         created.raw=comment$created,
         updated.raw=comment$updated),
    JiraUser(comment$author, "author"),
    JiraUser(comment$updateAuthor, "update.author")) %>% RemoveNANames
}

#' Jira Comments
#'
#' Parses several issue comments.
#'
#' @param issue List representing the issue (parsed from JSON).
#' @return A \code{data.table} representing the issue comments.
JiraComments <- function(issue) {
  comments <- issue$data$fields$comment$comments
  if (length(comments)) {
    comments <- lapply(comments, JiraComment) %>% rbindlist(fill=TRUE)
    cbind(comments, issue.id=as.integer(issue$data$id))
  }
}

#' Jira User
#'
#' Parses Jira user.
#'
#' @param person A list representing a person.
#' @param type Type of person (e.g. author). This is added as a prefix
#'   of the elements names.
#' @return The person with proper names.
JiraUser <- function(person, type="") {
  if (!is.null(person)) {
    cols <- c("key", "name", "displayName", "emailAddress", "timeZone")
    person <- person[cols]
    names(person) <- c("key", "name", "displayname", "email", "tz")
    if (type != "") {
      names(person) <- paste(type, names(person), sep=".")
    }
    person
  }
}

#' Jira Issue
#'
#' Parses a Jira Issue
#'
#' @param issue Issue as a list (parsed from JSON).
#' @return List of fields.
JiraIssue <- function(issue) {
  c(issue$data$field[c("summary", "description", "key")],
    list(issue.id=as.integer(issue$data$id),
         issue.key=issue$data$key,
         created.raw=issue$data$fields$created,
         updated.raw=issue$data$fields$updated,
         votes=issue$data$fields$votes$votes,
         status=issue$data$fields$status$name,
         priority=issue$data$fields$priority$name,
         issuetype=issue$data$fields$issuetype$name,
         resolution=issue$data$fields$resolution$name,
         component=JiraComponents(issue),
         product.name=issue$data$fields$project$name,
         tag=issue$tag, backend=issue$backend_name),
    unlist(mapply(JiraUser, with(issue$data$fields, list(assignee, reporter, creator)),
                  c("assignee", "reporter", "creator"), SIMPLIFY=FALSE),
           recursive=FALSE)) %>% RemoveNANames
}

#' Parse Jira
#'
#' Parse a JSON log representing a set of Jira issues.
#'
#' @param source Source of the issue tracker (Apache).
#' @param product Product tag of the issues.
#' @param file.in JSON file (possibly gzipped) containing the issues.
#' @param files.out List of output Parquet files for the issues,
#'   comments and history.
#' @export
ParseJira <- function(source, product, file.in, files.out=NULL) {
  logging::loginfo("Reading Jira log %s", file.in)
  log <- jsonlite::read_json(file.in)
  res <- list(issues=JiraIssue, versions=JiraVersions,
              history=JiraHistory, comments=JiraComments)
  res <- mapply(function(type, FUNC) {
    logging::loginfo("Extracting %s", type)
    log %>% lapply(FUNC) %>% rbindlist(fill=TRUE) %>% cbind(source, product)
  }, names(res), res, SIMPLIFY=FALSE)

  res$issues <- (res$issues %>%
                 JiraTime("created") %>%
                 JiraTime("updated") %>%
                 SubsetColumns(jira.columns$issues))
  res$comments <- (res$comments %>%
                   JiraTime("created") %>%
                   JiraTime("updated") %>%
                   SubsetColumns(jira.columns$comments))

  if (!is.null(files.out) && all(names(res) %in% names(files.out))) {
    for (type in names(res)) {
      WriteParquet(res[[type]], files.out[[type]])
    }
    data.table(source, product, type=names(res), filename=files.out[names(res)])
  } else res
}
