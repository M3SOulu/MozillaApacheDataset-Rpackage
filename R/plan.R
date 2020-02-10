#' Git Plan
#'
#' Returns a drake plan for git repositories
#'
#' @param repos Repository \code{data.table} object.
#' @param datadir Directory where the data is stored.
#' @return A drake plan tibble.
#' @export
GitPlan <- function(repos, datadir) {
  p <- drake_plan
  input <- File("git", repos, datadir, "raw", ext="json")
  log.out <- File("git", repos, datadir, "raw", "log")
  diff.out <- File("git", repos, datadir, "raw", "diff")
  agg.out <- file.path(datadir, "raw/git.parquet")
  p(git=target(ParseGitRepository(source, repo, file_in(input),
                                  file_out(log), file_out(diff)),
               transform=map(source=!!repos[type == "git", source],
                             repo=!!repos[type == "git", repo],
                             sub=!!repos[type == "git", sub],
                             gzip=!!repos[type == "git", gzip],
                             input=!!input,
                             log=!!log.out,
                             diff=!!diff.out,
                             .id=c(source, repo))),
    gitlog=target(Aggregate(list(git), SimplifyGitlog,
                            file.out=file_out(!!agg.out)),
                  transform=combine(git)))
}

#' Jira Plan
#'
#' Returns a drake plan for Jira.
#'
#' @param repos Repository \code{data.table} object.
#' @param datadir Directory where the data is stored.
#' @return A drake plan tibble.
#' @export
JiraPlan <- function(repos, datadir) {
  p <- drake_plan
  input <- File("jira", repos, datadir, "raw", ext="json")
  files.out <- list(issues="issues", versions="versions",
                    history="history", comments="comments")
  files.out <- lapply(files.out,
                      function(f) File("jira", repos, datadir, "raw", f))
  p(jira=target(ParseJira(source, repo, file_in(input),
                          list(issues=file_out(issues.out),
                               versions=file_out(versions.out),
                               history=file_out(history.out),
                               comments=file_out(comments.out))),
                transform=map(source=!!repos[type == "jira", source],
                              repo=!!repos[type == "jira", repo],
                              input=!!input,
                              issues.out=!!files.out$issues,
                              versions.out=!!files.out$versions,
                              history.out=!!files.out$history,
                              comments.out=!!files.out$comments,
                              .id=c(source, repo))),
    jiralog=target(rbindlist(list(jira)), transform=combine(jira)))
}

#' Bugzila Plan
#'
#' Returns a drake plan for Bugzillla.
#'
#' @param repos Repository \code{data.table} object.
#' @param datadir Directory where the data is stored.
#' @return A drake plan tibble.
#' @export
BugzillaPlan <- function(repos, datadir) {
  p <- drake_plan
  input <- File("bugzilla", repos, datadir, "raw", ext="json")
  files.out <- list(issues="issues", history="history", comments="comments")
  files.out <- lapply(files.out,
                      function(f) File("bugzilla", repos, datadir, "raw", f))
  p(bugzilla=target(ParseBugzilla(source, repo, file_in(input),
                                  list(issues=file_out(issues.out),
                                       history=file_out(history.out),
                                       comments=file_out(comments.out))),
                    transform=map(source=!!repos[type == "bugzilla", source],
                                  repo=!!repos[type == "bugzilla", repo],
                                  input=!!input,
                                  issues.out=!!files.out$issues,
                                  history.out=!!files.out$history,
                                  comments.out=!!files.out$comments,
                                  .id=c(source, repo))),
    bugzillalog=target(rbindlist(list(bugzilla)), transform=combine(bugzilla)))
}

#' Issue Plan
#'
#' Returns a drake plan for aggregating issue and comment logs.
#'
#' @param datadir Directory where the data is stored.
#' @return A drake plan tibble.
#' @export
IssuePlan <- function(datadir) {
  p <- drake_plan
  issues.out <- file.path(datadir, "raw/issues.parquet")
  comments.out <- file.path(datadir, "raw/comments.parquet")
  p(issuelog=AggregateIssues(jiralog, bugzillalog, "issues", identity,
                             file_out(!!issues.out)),
    commentlog=AggregateIssues(jiralog, bugzillalog, "comments", RemoveTextCols,
                               file_out(!!comments.out)))
}

#' Models Plan
#'
#' Returns a drake plan for NLoN and Senti4SD models.
#'
#' @return A drake plan tibble.
#' @export
ModelsPlan <- function() {
  drake_plan(nlon.model=NLoN::DefaultNLoNModel(),
             senti4sd.model=RSenti4SD::Senti4SDModel())
}

#' Map Comments Plan
#'
#' Returns a drakw plan for applying a function on issue comments.
#'
#' @param FUNC Function to apply on comments.
#' @param target.name Base name to use for the drake targets.
#' @param input.subdir Subdirectory of the input comments.
#' @param input.subname File sub name of the input comments.
#' @param output.subdir Subdirectory of the output comments.
#' @param output.subname File sub name of the output comments.
#' @param repos Repository \code{data.table} object.
#' @param datadir Directory where the data is stored.
#' @param types Types of issue repository to use.
#' @return A drake plan tibble.
MapCommentsPlan <- function(FUNC, target.name,
                            input.subdir, input.subname,
                            output.subdir, output.subname,
                            repos, datadir, types=c("bugzilla", "jira")) {
  input <- File(types, repos, datadir, input.subdir, input.subname)
  output <- File(types, repos, datadir, output.subdir, output.subname)
  p <- drake_plan
  p <- p(name=target((!!FUNC)(file_in(input), file_out(output)),
                     transform=map(source=!!repos[type %in% types, source],
                                   repo=!!repos[type %in% types, repo],
                                   input=!!input, output=!!output,
                                   .id=c(source, repo))))
  p$target <- sub("^name", target.name, p$target)
  p
}

#' NLP Plans
#'
#' Returns a list of map comments plans to build.
#'
#' @return List of lists with names target, input.subdir,
#'   input.subname and output.subname.
#' @seealso MapCommentsPlan
NLPPlans <- function() {
  nlp.plans <- rbindlist({
    list(list(target="comments", input.subdir="raw",
              input.subname="comments", output.subname=""),
         list(target="nlcomments", input.subdir="nlp",
              input.subname="", output.subname="nlcomments"),
         list(target="emoticons", input.subdir="nlp",
              input.subname="nlcomments"),
         list(target="sentences", input.subdir="nlp",
              input.subname="nlcomments"),
         list(target="sentistrength", input.subdir="nlp",
              input.subname="sentences"),
         list(target="senti4sd", input.subdir="nlp",
              input.subname="sentences"))
  }, fill=TRUE)
  nlp.plans[is.na(output.subname), output.subname := target]
  nlp.plans
}

#' NLP Plan Functions
#'
#' Functions for building comments map plans.
#'
#' @param senti4sd.chunk.limit Maximum number of comments to process at the
#'   same time with Senti4SD.
#' @param senti4sd.memory.limit Maximum amount of memory (in GB) to use for one
#'   run of Senti4SD. Overrides \code{senti4sd.chunk.size} by setting
#'   it to \code{500 * senti4sd.memory.limit}.
#' @return A list of functions.
NLPPlanFunctions <- function(senti4sd.chunk.limit=1000,
                             senti4sd.memory.limit=0) {
  list(comments=quote(function(fin, fout) {
    ProcessComments(ProcessRawComments, fin, fout,
                    nlon.model=nlon.model, chunk.limit=0)
  }), nlcomments=quote(function(fin, fout) {
    ProcessComments(NLComments, fin, fout)
  }), sentences=quote(function(fin, fout) {
    ProcessComments(SplitSentences, fin, fout)
  ## }), pos=quote(function(fin, fout) {
  ##   ProcessComments(POSTagging, fin, fout)
  }), emoticons=quote(function(fin, fout) {
    ProcessComments(Emoticons, fin, fout)
  }), sentistrength=quote(function(fin, fout) {
    ProcessComments(SentiStrength, fin, fout)
  }), senti4sd=substitute(function(fin, fout) {
    ProcessComments(MozillaApacheDataset::Senti4SD, fin, fout,
                    senti4sd.model, memory.limit, chunk.limit)
  }, list(path=senti4sd.path, memory.limit=senti4sd.memory.limit,
          chunk.limit=senti4sd.chunk.limit)))
}

#' NLP Plan
#'
#' Returns a drake plan for NLP of issue comments.
#'
#' @param repos Repository \code{data.table} object.
#' @param datadir Directory where the data is stored.
#' @param senti4sd.chunk.limit Maximum number of comments to process at the
#'   same time with Senti4SD.
#' @param senti4sd.memory.limit Maximum amount of memory (in GB) to use for one
#'   run of Senti4SD. Overrides \code{senti4sd.chunk.size} by setting
#'   it to \code{500 * senti4sd.memory.limit}.
#' @param types Types of issue repository to use.
#' @return A drake plan tibble.
#' @export
NLPPlan <- function(repos, datadir, senti4sd.chunk.limit=1000,
                    senti4sd.memory.limit=0, types=c("bugzilla", "jira")) {
  Functions <- NLPPlanFunctions(senti4sd.chunk.limit,
                                senti4sd.memory.limit)
  with(NLPPlans(), bind_plans({
    mapply(function(FUNC, target, input.subdir, input.subname, output.subname) {
      MapCommentsPlan(FUNC, target, input.subdir, input.subname,
                      "nlp", output.subname, repos, datadir, types)
    }, Functions[target], target, input.subdir, input.subname, output.subname,
    SIMPLIFY=FALSE)
  }))
}

#' NLP Aggregate Plan
#'
#' Returns a drake plan for aggregating NLP plans.
#'
#' @param repos Repository \code{data.table} object.
#' @param datadir Directory where the data is stored.
#' @param target Name of the target.
#' @param types Types of issue repository to use.
#' @return A drake plan tibble.
#' @export
NLPAggregatePlan <- function(repos, datadir, target,
                             types=c("bugzilla", "jira")) {
  files.in <- File(types, repos, datadir, "nlp", target)
  file.out <- file.path(datadir, sprintf("nlp/%s.parquet", target))
  FUNC <- function(f) {
    res <- ReadParquet(f)
    if (ncol(res) > 2) res
  }
  p <- drake_plan(target=Aggregate(file_in(!!files.in), !!FUNC,
                                   file.out=file_out(!!file.out)))
  p$target <- target
  p
}

#' Identity Merging Plan
#'
#' Returns a drake plan for identity merging
#'
#' @param datadir Directory where the data is stored.
#' @return A drake plan tibble.
#' @export
IdentityMergingPlan <- function(datadir) {
  ids.out <- file.path(datadir, "identities.parquet")
  merging.out <- file.path(datadir, "idmerging.parquet")
  File <- function(f) file.path(datadir, "raw", sprintf("%s.parquet", f))
  drake_plan(identities.issues=IssuesIdentities(ReadParquet(file_in(!!File("issues")))),
             identities.comments=IssueCommentsIdentities(ReadParquet(file_in(!!File("comments")))),
             identities.commits=CommitsIdentities(ReadParquet(file_in(!!File("git")))),
             identities=Identities(identities.commits,
                                   identities.issues,
                                   identities.comments,
                                   file_out(!!ids.out)),
             idmerging=IdentityMerging(identities, file_out(!!merging.out)))
}

#' Log Plan
#'
#' Returns a drake plan for the different final log files
#'
#' @param datadir Directory where the data is stored.
#' @return A drake plan tibble.
#' @export
LogPlan <- function(datadir) {
  raw.git <- file.path(datadir, "raw/git.parquet")
  commits <- file.path(datadir, "commits.parquet")
  raw.issues <- file.path(datadir, "raw/issues.parquet")
  raw.comments <- file.path(datadir, "raw/comments.parquet")
  idmerging <- file.path(datadir, "idmerging.parquet")
  tzhistory <- file.path(datadir, "tzhistory.parquet")
  timestamps <- file.path(datadir, "timestamps.parquet")
  drake_plan(commits=CommitLog(file_in(!!raw.git),
                               file_in(!!raw.issues),
                               file_out(!!commits)),
             tzhistory=CommitTimeZoneHistory(file_in(!!commits),
                                             file_in(!!idmerging),
                                             file_out(!!tzhistory)),
             timestamps=Timestamps(file_in(!!commits),
                                   file_in(!!raw.issues),
                                   file_in(!!raw.comments),
                                   file_in(!!idmerging),
                                   file_in(!!tzhistory),
                                   file_out(!!timestamps)))
}

#' Full Plan
#'
#' Returns the full drake plan.
#'
#' @param datadir Directory where the data is stored.
#' @param senti4sd.chunk.limit Maximum number of comments to process at the
#'   same time with Senti4SD.
#' @param senti4sd.memory.limit Maximum amount of memory (in GB) to use for one
#'   run of Senti4SD. Overrides \code{senti4sd.chunk.size} by setting
#'   it to \code{500 * senti4sd.memory.limit}.
#' @return A drake plan tibble.
#' @export
FullPlan <- function(datadir, senti4sd.chunk.limit=1000,
                     senti4sd.memory.limit=0) {
  repos <- RawData(datadir)
  bind_plans(GitPlan(repos, datadir),
             JiraPlan(repos, datadir),
             BugzillaPlan(repos, datadir),
             IssuePlan(datadir),
             ModelsPlan(),
             IdentityMergingPlan(datadir),
             LogPlan(datadir),
             NLPPlan(repos, datadir, senti4sd.chunk.limit,
                     senti4sd.memory.limit),
             NLPAggregatePlan(repos, datadir, "sentistrength"),
             NLPAggregatePlan(repos, datadir, "senti4sd"),
             NLPAggregatePlan(repos, datadir, "emoticons"))
}
