#' Process chunk
#'
#' Processes chunks of comments with a function
#'
#' @param comments Comments \code{data.table} object.
#' @param FUNC Function to apply on comments.
#' @param limit Maximum number of comments to process in one chunk.
#' @param ... Addition parameters to pass to FUNC.
#' @return Comments with added columns from processing with FUNC.
ProcessChunk <- function(comments, FUNC, limit, ...) {
  if (nrow(comments)) {
    group <- 1:nrow(comments) %/% limit
    res <- rbindlist(lapply(unique(group), function(i) {
      logging::loginfo(Sys.time())
      logging::loginfo("%d/%d", i + 1, length(unique(group)))
      text <- comments[group == i, text]
      FUNC(text, ...)
    }))
    cols <- !names(comments) %in% names(res)
    cbind(comments[, cols, with=FALSE], res)
  } else comments
}

#' Make NLoN Model.
#'
#' Builds \code{NLoN} model.
#'
#' @return \code{NLoN} model.
#' @export
MakeNLoNModel <- function() {
  with(NLoN::nlon.data, NLoN::NLoNModel(text, rater2))
}

#' NLoN
#'
#' Use NLoN model to predict text.
#'
#' @param text The text.
#' @param model The NLoN model.
#' @return \code{data.table} object with nlon boolean column and
#'   number of characters.
NLoN <- function(text, model) {
  logging::loginfo("Running NLoN")
  t <- system.time(nlon <- NLoN::NLoNPredict(model, text))
  logging::loginfo("%f seconds ellapsed", t[3])
  data.table(nlon=nlon[, 1], nchar=nchar(text))
}

#' RunNLoN
#'
#' Run NLoN on comments by chunk.
#'
#' @param comments \code{data.table} object with comments.
#' @param model NLoN model.
#' @param limit Maximum number of comments to process in one chunk.
#' @return Processed comments with NLoN.
RunNLoN <- function(comments, model, limit) {
  ProcessChunk(comments, NLoN, limit, model)
}

#' Classify text lines
#'
#' Classifies text lines.
#'
#' @param text Text.
#' @param raw Raw text.
#' @return A numeric vector where 1 means author text, 2 quoted text
#'   and 3 generated text.
ClassifyTextLines <- function(text, raw) {
  ## 1 is author text, 2 is quote and 3 generated text
  if (length(text)) {
    res <- sapply(text, function(line) {
      if (length(raw) && raw[[1]] == line) {
        raw <<- raw[-1]
        1
      } else 3
    })
    res[grepl("^(\\(in reply to |>)", text, ignore.case=TRUE) &
        res == 1] <- 2
    res
  } else numeric(0)
}

#' Apply functions
#'
#' Applies functions to comments.
#'
#' @param comments \code{data.table} object with comments.
#' @param FUNC Function or list of functions to successively apply.
#' @param ... Additional parameters to give to FUNC.
#' @return The comments with the function(s) applied.
ApplyFunctions <- function(comments, FUNC, ...) {
    if (is.function(FUNC)) {
      FUNC <- list(FUNC)
    }
    if (!is.list(FUNC)) stop("FUNC must be a function or a list of functions.")
      for (F in FUNC) {
        if (nrow(comments)) {
          comments <- F(comments, ...)
        } else break
    }
    comments
}

#' Process Jira Comments
#'
#' Processes Jira comments
#'
#' @param comments \code{data.table} object with comments.
#' @return Processed Jira comments.
ProcessJiraComments <- function(comments) {
  comments <- comments[, list(source=project, bug.id, comment.id=1:.N,
                              text=strsplit(body, "\n"), line.type=1)]
  tidyr::unnest(comments)
}

#' Process Bugzilla comments
#'
#' Processes Bugzilla comments.
#'
#' @param comments \code{data.table} object with comments.
#' @return Processed Bugzilla comments.
ProcessBugzillaComments <- function(comments) {
  comments <- comments[, list(source=project, bug.id=id, comment.id=1:.N,
                              text, raw=raw_text)]
  comments[, text := strsplit(text, "\n")]
  comments[, raw := strsplit(raw, "\n")]
  comments[, {
    list(text=text[[1]], line.type=ClassifyTextLines(text[[1]], raw[[1]]))
  }, by=c("source", "bug.id", "comment.id")]
}

## ProcessRawComments <- function(files.in, files.out, FUNC) {
##   projects <- sub("^comments-(.+)\\.csv\\.gz$", "\\1",
##                   basename(unlist(files.in)))
##   names(files.in) <- projects
##   names(files.out) <- sub("^(.+)\\.rds$", "\\1",
##                           basename(unlist(files.out)))
##   for (project in projects) {
##     logging::loginfo("Reading comments text for %s", project)
##     comments <- ReadFile(files.in[[project]])
##     comments <- ApplyFunctions(comments, FUNC)
##     saveRDS(comments, files.out[[project]])
##   }
##   invisible(NULL)
## }

## ProcessNLoN <- function(jira.in, bugzilla.in, dir.out, limit=100000) {
##   nlon.model <- with(NLoN::nlon.data, NLoN::NLoNModel(text, rater2))
##   NLoN <- function(comments) RunNLoN(comments, nlon.model, limit=limit)
##   ProcessRawComments(jira.in, dir.out, list(ProcessJiraComments, NLoN))
##   ProcessRawComments(bugzilla.in, dir.out, list(ProcessBugzillaComments, NLoN))
##   invisible(NULL)
## }

#' Process NLoN
#'
#' Processes comments with NLoN
#'
#' @param file.in Input RDS file.
#' @param file.out Output RDS file.
#' @param src Source (jira or bugzilla).
#' @param nlon.model NLoN model.
#' @param limit Maximum number of comments to process at the same time.
#' @export
ProcessNLoN <- function(file.in, file.out, src, nlon.model, limit=100000) {
  NLoN <- function(comments) RunNLoN(comments, nlon.model, limit=limit)
  comments <- ReadFile(file.in)
  Process <- {
    if (src =="jira") ProcessJiraComments
    else if (src == "bugzilla") ProcessBugzillaComments
    else stop(sprintf("Unknown source %s", src))
  }
  comments <- ApplyFunctions(comments, list(Process, NLoN))
  saveRDS(comments, file.out)
  invisible(NULL)
}

#' Split sentences
#'
#' Split comments by detecting sentences with openNLP.
#'
#' @param comments \code{data.table} object with comments.
#' @param limit Maximum bumber of comments to process at the same time.
#' @param nclusters Number of processes to use.
#' @return Comments with one row per sentence.
#' @export
SplitSentences <- function(comments, limit=100000,
                           nclusters=parallel::detectCores()) {
  cl <- makeCluster(nclusters)
  clusterEvalQ(cl, {
    library(openNLP)
    library(NLP)
    library(data.table)
    sent_annotator <- Maxent_Sent_Token_Annotator()
  })

  logging::loginfo("Tokenization")
  t <- system.time({
    comments <- ProcessChunk(comments, function(text) {
      data.table(sentences=parLapply(cl, text, function(text) {
        if (nchar(text)) {
          try({
            tokenized <- annotate(text, sent_annotator)
            as.data.table(tokenized)[type == "sentence", list(start, end)]
          })
        }
      }))
    }, limit)
  })
  logging::loginfo("%f seconds ellapsed", t[3])

  stopCluster(cl)
  comments <- tidyr::unnest(comments[sapply(comments$sentences, is.data.frame)])
  comments[, text := gsub("\\s+", " ", substr(text, start, end))]
  comments$start <- NULL
  comments$end <- NULL
  comments[!is.na(text)]
}

#' POS tagging
#'
#' Pos tag comments with openNLP.
#'
#' @param comments \code{data.table} object with comments.
#' @param limit Maxmimum number of comments to process at the same time.
#' @param nclusters Number of processes to use.
#' @return POS tagging result.
#' @export
POSTagging <- function(comments, limit=100000,
                       nclusters=parallel::detectCores()) {
  cl <- makeCluster(nclusters)
  clusterEvalQ(cl, {
    library(openNLP)
    library(NLP)
    library(data.table)
    sent_annotator <- Maxent_Sent_Token_Annotator()
    word_annotator <- Maxent_Word_Token_Annotator()
    pos_annotator <- Maxent_POS_Tag_Annotator()
  })

  logging::loginfo("POS Tagging")
  t <- system.time({
    comments <- ProcessChunk(comments, function(text) {
      data.table(sentences=parLapply(cl, text, function(text) {
        if (nchar(text)) {
          try({
            tokenized <- annotate(text, list(sent_annotator, word_annotator))
            res <- as.data.table(annotate(text, pos_annotator, tokenized))
            res[type == "word", list(start, end,
                                     pos=sapply(features, function(x) x$POS))]
          })
        }
      }))
    }, limit)
  })
  logging::loginfo("%f seconds ellapsed", t[3])

  stopCluster(cl)
  comments <- tidyr::unnest(comments[sapply(comments$sentences, is.data.frame)])
  comments[, text := substr(text, start, end)]
  comments$start <- NULL
  comments$end <- NULL
  comments[!is.na(text)]
}

#' Natural language comments
#'
#' Subsets comments that are NL.
#'
#' @param comments \code{data.table} object with comments.
#' @return Natural language comments.
#' @export
NaturalLanguageComments <- function(comments) {
  comments <- comments[nlon == "NL" & line.type == 1,
                       list(text=paste(text, collapse="\n")),
                       by=c("source", "bug.id", "comment.id")]
  comments[, text := CleanText(text)]
  comments <- comments[nchar(text) > 0]
}

#' Process comments.
#'
#' Process comments with a function.
#'
#' @param file.in Input RDS file.
#' @param file.out Output RDS file.
#' @param FUNC The function.
#' @param ... Additional parameters to pass to the function.
#' @export
ProcessComments <- function(file.in, file.out, FUNC, ...) {
  comments <- readRDS(file.in)
  comments <- ApplyFunctions(comments, FUNC, ...)
  saveRDS(comments, file.out)
  invisible(NULL)
}

#' Aggregate comments
#'
#' Aggregate comments.
#'
#' @param files.in Input RDS files.
#' @param file.out Output RDS file.
#' @param FUNC The function.
#' @param ... Additional parameters to pass to the function.
#' @export
AggregateComments <- function(files.in, file.out, FUNC, ...) {
  res <- rbindlist(lapply(files.in, function(f.in) {
    logging::loginfo(f.in)
    comments <- readRDS(f.in)
    ApplyFunctions(comments, FUNC, ...)
  }))
  saveRDS(res, file.out)
  invisible(NULL)
}

#' POS Metrics
#'
#' Computes POS metrics on POS tagging result.
#'
#' @param files.in Input RDS files of POS tagging.
#' @param file.out Output RDS file.
#' @export
POSMetrics <- function(files.in, file.out) {
  AggregateComments(files.in, file.out, function(comments) {
    comments[, list(nwords=.N,
                    unique.words=length(unique(text)),
                    nsentences=sum(pos == "."),
                    nchar=sum(nchar(text)),
                    exclamations=sum(pos == "." & text == "!"),
                    questions=sum(pos == "." & text == "?"),
                    ## proper.nouns=sum(pos == "NNP" | pos == "NNPS"),
                    npos=length(unique(pos))),
             by=c("source", "bug.id", "comment.id")]
  })
}

#' Aggregate POS
#'
#' Aggregate POS tagging result
#'
#' @param files.in Input RDS files of POS tagging.
#' @param buglog.in Bug log input RDS file.
#' @param file.out Output RDS file.
#' @export
AggregatePOS <- function(files.in, buglog.in, file.out) {
  pos.tags <- c("``", ",", ":", ".", "''", "$", "#", "CC", "CD", "DT",
                "EX", "FW", "IN", "JJ", "JJR", "JJS", "-LRB-", "LS",
                "MD", "NN", "NNP", "NNPS", "NNS", "PDT", "POS", "PRP",
                "PRP$", "RB", "RBR", "RBS", "RP", "-RRB-", "SYM", "TO",
                "UH", "VB", "VBD", "VBG", "VBN", "VBP", "VBZ", "WDT",
                "WP", "WP$", "WRB")

  buglog <- readRDS(buglog.in)
  buglog <- buglog[, list(source, bug.id=as.integer(bug.id), comment.id)]
  AggregateComments(files.in, file.out, function(comments) {
    comments <- merge(comments, buglog, by=c("source", "bug.id", "comment.id"))
    comments[, pos := factor(pos, levels=pos.tags)]
    comments[, .N, by=c("source", "bug.id", "comment.id", "pos")]
  })
}

#' Emoticons
#'
#' Detects emoticons from comments.
#'
#' @param files.in Input RDS files.
#' @param file.out Output RDS file.
#' @export
Emoticons <- function(files.in, file.out) {
  AggregateComments(files.in, file.out, list(function(comments) {
    setkey(comments, source, bug.id, comment.id)
    emotionFindeR::EmoticonsAndEmojis(comments)
  }, emotionFindeR::TextBeforeAfterEmoticons,
  emotionFindeR::EmoticonsSentiStrength))
}

#' SentiStrength
#'
#' Runs SentiStrength on comments.
#'
#' @param files.in Input RDS files.
#' @param file.out Output RDS file.
#' @param limit Maximum number of comments to process at the same time.
#' @export
SentiStrength <- function(files.in, file.out, limit=100000) {
  AggregateComments(files.in, file.out, function(comments) {
    comments <- ProcessChunk(comments, emotionFindeR::RunSentiAll,
                             limit, "text")
    ## comments <- cbind(comments, emotionFindeR::RunSentiAll(comments$text, "text"))
    comments$text <- NULL
    comments
  })
}
