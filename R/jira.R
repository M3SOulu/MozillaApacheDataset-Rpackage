JiraVersions <- function(issue){
  if (length(issue$data$fields$versions)) {
    cbind(rbindlist(lapply(issue$data$fields$versions, function(v) {
      RemoveNANames(v[c("releaseDate", "released", "name", "archived")])
    }), fill=TRUE), issue.id=issue$data$id)
  }
}

JiraComponents <- function(issue) {
  components <- sapply(issue$data$fields$components, function(c) c$name)
  paste(components, collapse="\n")
}

JiraHistoryItems <- function(items) {
  lapply(items, function(item) {
    items <- item[c("field", "fieldtype", "fromString", "toString")]
    names(items) <- c("field", "fieldtype", "from", "to")
    items
  }) %>% rbindlist(fill=TRUE)
}

JiraHistory <- function(issue) {
  history <- issue$data$changelog$histories
  if (length(history)) {
    lapply(history, function(h) {
      c(JiraHistoryItems(h$items), list(time=h$created), JiraUser(h$author))
    }) %>% rbindlist(fill=TRUE) %>% cbind(issue.id=issue$data$id)
  }
}

JiraComment <- function(comment) {
  c(list(comment.id=comment$id,
         text=comment$body),
    comment[c("created", "updated")],
    JiraUser(comment$author, "author"),
    JiraUser(comment$updateAuthor, "update.author")) %>% RemoveNANames
}

JiraComments <- function(issue) {
  comments <- issue$data$fields$comment$comments
  if (length(comments)) {
    comments <- lapply(comments, JiraComment) %>% rbindlist(fill=TRUE)
    cbind(comments, issue.id=issue$data$id)
  }
}

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

JiraIssue <- function(issue) {
  c(issue[c("tag", "backend_name")],
    issue$data$field[c("created", "summary", "description", "key")],
    list(issue.id=issue$data$id,
         votes=issue$data$fields$votes$votes,
         status=issue$data$fields$status$name,
         priority=issue$data$fields$priority$name,
         issuetype=issue$data$fields$issuetype$name,
         resolution=issue$data$fields$resolution$name,
         component=JiraComponents(issue),
         repo.name=issue$data$fields$project$name),
    unlist(mapply(JiraUser, with(issue$data$fields, list(assignee, reporter, creator)),
                  c("assignee", "reporter", "creator"), SIMPLIFY=FALSE),
           recursive=FALSE)) %>% RemoveNANames
}

ParseJira <- function(source, repo, file.in, files.out=NULL) {
  logging::loginfo("Reading Jira log %s", file.in)
  log <- jsonlite::read_json(file.in)
  res <- list(issues=JiraIssue, versions=JiraVersions,
              history=JiraHistory, comments=JiraComments)
  res <- mapply(function(type, FUNC) {
    logging::loginfo("Extracting %s", type)
    log %>% lapply(FUNC) %>% rbindlist(fill=TRUE) %>% cbind(source, repo)
  }, names(res), res, SIMPLIFY=FALSE)

  if (!is.null(files.out) && all(names(res) %in% names(files.out))) {
    for (type in names(res)) {
      WriteParquet(res[[type]], files.out[[type]])
    }
    data.table(source, repo, type=names(res), filename=files.out[names(res)])
  } else res
}
