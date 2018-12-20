#' Aggregate Plan
#'
#' Make a plan for aggregating raw data.
#'
#' @param datadir Directory where data is stored.
#' @return a drake plan.
#' @export
AggregatePlan <- function(datadir) {
  plan <- drake_plan(agg.git=AggregateGit(raw.git.log, raw.git.diff,
                                          file_out("DATADIR__/raw/git.rds")),
                     agg.jira.bugs=AggregateJiraBugs(raw.jira.bugs, raw.jira.components,
                                                     file_out("DATADIR__/raw/jira-bugs.rds")),
                     agg.jira.versions=AggregateJiraVersions(raw.jira.versions,
                                                             file_out("DATADIR__/raw/jira-versions.rds")),
                     agg.jira.comments=AggregateJiraComments(raw.jira.comments,
                                                             file_out("DATADIR__/raw/jira-comments.rds")),
                     agg.bz.bugs=AggregateBugzillaBugs(raw.bugzilla.bugs,
                                                       file_out("DATADIR__/raw/bugzilla-bugs.rds")),
                     agg.bz.comments=AggregateBugzillaComments(raw.bugzilla.comments,
                                                               file_out("DATADIR__/raw/bugzilla-comments.rds")))
  evaluate_plan(plan, list(DATADIR__=datadir), rename=FALSE)
}

#' Identities Plan
#'
#' Make a plan for identity merging.
#'
#' @param datadir Directory where data is stored.
#' @return a drake plan.
#' @export
IdentitiesPlan <- function(datadir) {
  plan <- drake_plan(identities=Identities(file_in("DATADIR__/raw/git.rds"),
                                           file_in("DATADIR__/raw/bugzilla-comments.rds"),
                                           file_in("DATADIR__/raw/jira-comments.rds"),
                                           file_out("DATADIR__/identities_commits.csv"),
                                           file_out("DATADIR__/identities_bugs.csv"),
                                           file_out("DATADIR__/identities.csv")),
                     idmerging=IdentityMerging(file_in("DATADIR__/identities.csv"),
                                               file_out("DATADIR__/identity_merging.csv")),
                     mozdev=MozillaDevelopers(file_in("DATADIR__/identity_merging.csv"),
                                              file_in("DATADIR__/mozilla_developers.csv")))
  evaluate_plan(plan, list(DATADIR__=datadir), rename=FALSE)
}

#' Log Plan
#'
#' Make a plan for making git and bug logs.
#'
#' @param datadir Directory where data is stored.
#' @return a drake plan.
#' @export
LogPlan <- function(datadir) {
  plan <- drake_plan(gitlog=GitLog(file_in("DATADIR__/raw/git.rds"),
                                   file_in("DATADIR__/raw/bugzilla-bugs.rds"),
                                   file_in("DATADIR__/identity_merging.csv"),
                                   file_out("DATADIR__/git_filtered.rds")),
                     buglog=BugLog(file_in("DATADIR__/identity_merging.csv"),
                                   file_in("DATADIR__/raw/bugzilla-comments.rds"),
                                   file_in("DATADIR__/raw/bugzilla-bugs.rds"),
                                   file_in("DATADIR__/raw/jira-comments.rds"),
                                   file_in("DATADIR__/raw/jira-bugs.rds"),
                                   file_out("DATADIR__/buglog.rds")),
                     tz.history=GitTimeZoneHistory(file_in("DATADIR__/git_filtered.rds"),
                                                   file_out("DATADIR__/git-tz-history.rds")),
                     buglog2=FilteredBugLog(file_in("DATADIR__/buglog.rds"),
                                            file_in("DATADIR__/git-tz-history.rds"),
                                            file_out("DATADIR__/filtered_buglog.rds")),
                     combined=CombinedLog(file_in("DATADIR__/git_filtered.rds"),
                                          file_in("DATADIR__/filtered_buglog.rds"),
                                          file_in("DATADIR__/pos-metrics.rds"),
                                          file_out("DATADIR__/combined_log.rds")))
  evaluate_plan(plan, list(DATADIR__=datadir), rename=FALSE)
}

#' NLP Aggregate Plan
#'
#' Make a plan for aggregating natural language processing of issue comments.
#'
#' @param datadir Directory where data is stored.
#' @return a drake plan.
#' @export
NLPAggregatePlan <- function(datadir) {
  projects <- NLPProjects(datadir)
  plan <- drake_plan(pos.metrics=POSMetrics(bugpos,
                                            file_out("DATADIR__/pos-metrics.rds")),
                     pos=AggregatePOS(bugpos,
                                      file_in("DATADIR__/filtered_buglog.rds"),
                                      file_out("DATADIR__/pos.rds")),
                     emoticons=Emoticons(bugcomments,
                                         file_out("DATADIR__/emoticons.rds")),
                     sentistrength=SentiStrength(bugsentences,
                                                 file_out("DATADIR__/comments-valence-arousal.rds")))
  bind_plans(evaluate_plan(plan, list(DATADIR__=datadir), rename=FALSE),
             NLPInFiles("raw/bugcomments", "raw.bugcomments", datadir),
             NLPInFiles("bugcomments", "bugcomments", datadir),
             NLPInFiles("bugsentences", "bugsentences", datadir),
             NLPInFiles("bugpos", "bugpos", datadir))
}

#' NLP Plan
#'
#' Make a plan for natural langiage processing of issue comments.
#'
#' @param datadir Directory where data is stored.
#' @return a drake plan.
#' @export
NLPPlan <- function(datadir) {
  projects <- NLPProjects(datadir)
  plans <- list({
    plan <- drake_plan(process.nlon=ProcessNLoN(file_in("DATADIR__/raw/jira/comments-PRJ__.csv.gz"),
                                                file_out("DATADIR__/raw/bugcomments/PRJ__.rds"),
                                                "jira", nlon.model))
    evaluate_plan(plan, list(PRJ__=projects[source == "jira", project]))
  }, {
    plan <- drake_plan(process.nlon=ProcessNLoN(file_in("DATADIR__/raw/bugzilla/comments-PRJ__.csv.gz"),
                                                file_out("DATADIR__/raw/bugcomments/PRJ__.rds"),
                                                "bugzilla", nlon.model))
    evaluate_plan(plan, list(PRJ__=projects[source == "bugzilla", project]))
  }, {
    plan <- drake_plan(nlcomments=ProcessComments(file_in("DATADIR__/raw/bugcomments/PRJ__.rds"),
                                                  file_out("DATADIR__/bugcomments/PRJ__.rds"),
                                                  NaturalLanguageComments))
    evaluate_plan(plan, list(PRJ__=projects$project))
  }, {
    plan <- drake_plan(sentences=ProcessComments(file_in("DATADIR__/bugcomments/PRJ__.rds"),
                                                 file_out("DATADIR__/bugsentences/PRJ__.rds"),
                                                 SplitSentences))
    evaluate_plan(plan, list(PRJ__=projects$project))
  }, {
    plan <- drake_plan(run.pos=ProcessComments(file_in("DATADIR__/bugcomments/PRJ__.rds"),
                                               file_out("DATADIR__/bugpos/PRJ__.rds"),
                                               POSTagging))
    evaluate_plan(plan, list(PRJ__=projects$project))
  })
  bind_plans(evaluate_plan(bind_plans(plans), list(DATADIR__=datadir), rename=FALSE),
             drake_plan(nlon.model=MakeNLoNModel()))
}

#' List projects
#'
#' List projects of a specific type in specific folder.
#'
#' @param src Source of the projects (jira or bugzilla).
#' @param type Type of data (e.g. comments or bugs).
#' @param datadir Directory where data is stored.
#'
#' @return List of project names.
ListProjects <- function(src, type, datadir) {
  path <- file.path(datadir, "raw", src)
  file.re <- sprintf("^%s-(.+)\\.csv\\.gz$", type)
  sub(file.re, "\\1", dir(path, pattern=file.re))
}

#' NLP projects
#'
#' List Jira and Bugzilla projects with comments.
#'
#' @param datadir Directory where data is stored.
#' @return List of project names
#' @export
NLPProjects <- function(datadir) {
  rbind(data.table(source="jira", project=ListProjects("jira", "comments", datadir)),
        data.table(source="bugzilla", project=ListProjects("bugzilla", "comments", datadir)))
}

#' Raw Plans
#'
#' Make a plan for raw data files.
#'
#' @param src Source of the projects (jira or bugzilla).
#' @param type Type of data (e.g. comments or bugs).
#' @param datadir Directory where data is stored.
#' @return a drake plan.
#' @export
RawPlan <- function(src, type, datadir) {
  projects <- ListProjects(src, type, datadir)
  plan <- drake_plan(raw=file_in("DATADIR__/raw/SRC__/TYPE__-PRJ__.csv.gz"))
  plan <- evaluate_plan(plan, list(DATADIR__=datadir), rename=FALSE)
  plan <- evaluate_plan(plan, list(SRC__=src, TYPE__=type, PRJ__=projects))
  gather_plan(plan, sprintf("raw.%s.%s", src, type), append=TRUE)
}

#' NLP input files
#'
#' List NLP input files as a plan.
#'
#' @param root.dir Root directory.
#' @param target the target name in the plan.
#' @param datadir Directory where data is stored.
#' @return A drake plan.
NLPInFiles <- function(root.dir, target, datadir) {
  projects <- NLPProjects(datadir)
  plan <- drake_plan(raw=file_in("DIR__/PRJ__.rds"))
  plan <- evaluate_plan(plan, list(DIR__=file.path(datadir, root.dir),
                                   PRJ__=projects$project))
  gather_plan(plan, target, append=TRUE)
}

## NLPOutFiles <- function(root.dir, datadir, CMD="list(%s)") {
##   files <- file.path(datadir, root.dir, sprintf("%s.rds", NLPProjects(datadir)$project))
##   files <- sprintf("file_out(\"%s\")", files)
##   sprintf(CMD, paste(files, collapse=", "))
## }

#' Raw Plans
#'
#' Make a plan for raw data files.
#'
#' @param datadir Directory where data is stored.
#' @return a drake plan.
#' @export
RawPlans <- function(datadir) {
  bind_plans(RawPlan("bugzilla", "bugs", datadir),
             RawPlan("bugzilla", "comments", datadir),
             RawPlan("jira", "bugs", datadir),
             RawPlan("jira", "components", datadir),
             RawPlan("jira", "versions", datadir),
             RawPlan("jira", "comments", datadir),
             RawPlan("git", "log", datadir),
             RawPlan("git", "diff", datadir),
             drake_plan(raw.nlp=c(raw.bugzilla.comments, raw.jira.comments)))
}

#' Full Plan
#'
#' Make the full plan.
#'
#' @param datadir Directory where data is stored.
#' @return a drake plan.
#' @export
FullPlan <- function(datadir) {
  bind_plans(RawPlans(datadir),
             AggregatePlan(datadir),
             IdentitiesPlan(datadir),
             LogPlan(datadir),
             NLPPlan(datadir),
             NLPAggregatePlan(datadir))
}
