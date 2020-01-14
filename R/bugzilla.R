BugzillaUser <- function(person, type="") {
  if (!is.null(person)) {
    cols <- c("id", "name", "real_name", "email")
    person <- person[cols]
    names(person) <- c("key", "name", "displayname", "email")
    if (type != "") {
      names(person) <- paste(type, names(person), sep=".")
    }
    person
  }
}

BugzillaIssue <- function(issue) {
  c(issue[c("tag", "backend_name")],
    issue$data[c("id", "creation_time", "component", "status", "resolution",
                 "severity", "priority", "version", "target_milestone",
                 "cf_last_resolved", "votes", "summary")],
    list(issue.id=issue$data$id,
         created=issue$data$creation_time),
    BugzillaUser(issue$data$creator_detail, "creator"),
    BugzillaUser(issue$data$assigned_to_detail, "assignee")) %>% RemoveNANames
}

BugzillaHistoryItems <- function(items) {
  lapply(items, function(item) {
    items <- item[c("field_name", "added", "removed")]
    names(items) <- c("field", "from", "to")
    items
  }) %>% rbindlist(fill=TRUE)
}

BugzillaHistory <- function(issue) {
  history <- issue$data$history
  if (length(history)) {
    lapply(history, function(h) {
      c(BugzillaHistoryItems(h$changes),
        list(time=h$when, email=h$who))
    }) %>% rbindlist(fill=TRUE) %>% cbind(issue.id=issue$data$id)
  }
}

BugzillaComment <- function(comment) {
  with(comment, list(comment.id=id,
                     author.email=author,
                     creator.email=creator,
                     created=creation_time,
                     updated=time,
                     text=text,
                     raw.text=raw_text) %>% RemoveNANames)
}

BugzillaComments <- function(issue) {
  comments <- issue$data$comments
  if (length(comments)) {
    comments <- lapply(comments, BugzillaComment) %>% rbindlist(fill=TRUE)
    cbind(comments, issue.id=issue$data$id)
  }
}

ParseBugzilla <- function(source, repo, file.in, files.out=NULL) {
  logging::loginfo("Reading Bugzilla log %s", file.in)
  log <- jsonlite::read_json(file.in)
  res <- list(issues=BugzillaIssue, history=BugzillaHistory,
              comments=BugzillaComments)
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
