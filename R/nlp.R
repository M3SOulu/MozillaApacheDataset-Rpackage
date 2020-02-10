#' Process Text
#'
#' Processes text data with a function
#'
#' @param comments Comments \code{data.table} object.
#' @param FUNC Function to apply on comments.
#' @param chunk.limit Maximum number of comments to process in one
#'   chunk. 0 means no limit.
#' @param ... Addition parameters to pass to FUNC.
#' @return Comments with additional columns returned from processing
#'   the comments with FUNC.
#' @export
ProcessText <- function(comments, FUNC, chunk.limit=0, ...) {
  if (nrow(comments)) {
    if (chunk.limit > 0) {
      group <- 1:nrow(comments) %/% chunk.limit
      res <- rbindlist(lapply(unique(group), function(i) {
        logging::loginfo("Processing chunk %d/%d", i + 1, length(unique(group)))
        text <- comments[group == i, text]
        FUNC(text, ...)
      }))
    } else res <- FUNC(comments$text, ...)
    cols <- !names(comments) %in% names(res)
    cbind(comments[, cols, with=FALSE], res)
  } else comments
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
  logging::loginfo("Running NLoN on %d text documents", length(text))
  t <- system.time(nlon <- NLoN::NLoNPredict(model, text))
  logging::loginfo("%f seconds ellapsed", t[3])
  data.table(nlon=nlon[, 1])
}

#' Line Type
#'
#' Returns a factor corresponding to a specific line type
#'
#' @param type Type as a character string.
#' @return Type as a factor with levels author, quote and generated.
LineType <- function(type) {
  factor(type, levels=c("author", "quote", "generated"))
}

#' Classify text lines
#'
#' Classifies text lines.
#'
#' @param text Text.
#' @param raw Raw text.
#' @return A Line Type factor.
#' @seealso LineType
ClassifyTextLines <- function(text, raw) {
  ## 1 is author text, 2 is quote and 3 generated text
  if (length(text)) {
    res <- sapply(text, function(line) {
      if (length(raw) && raw[[1]] == line) {
        raw <<- raw[-1]
        LineType("author")
      } else LineType("generated")
    })
    res[grepl("^(\\(in reply to |>)", text, ignore.case=TRUE) &
        res == "author"] <- LineType("quote")
    res
  }
}

#' Process Comments
#'
#' Processes comments with a function.
#'
#' @param FUNC Function to apply.
#' @param file.in Input Parquet filename.
#' @param file.out Output Parquet filename.
#' @param ... Additional parameters to pass to \code{FUNC}.
#' @return The result of \code{FUNC} applied to the comments if
#'   \code{file.out} is NULL, or the output filename otherwise.
#' @export
ProcessComments <- function(FUNC, file.in, file.out=NULL, ...) {
  comments <- ReadParquet(file.in)
  if (nrow(comments) && ncol(comments) > 2) {
    result <- FUNC(comments, ...)
    if (nrow(result)) comments <- result
  }
  if (!is.null(file.out)) {
    WriteParquet(comments, file.out)
    file.out
  } else comments
}

#' Process Raw Comments
#'
#' Processes Apache and Mozilla raw comments and optionally runs NLoN.
#'
#' @param comments Raw comments to process.
#' @param nlon.model NLoN model.
#' @param chunk.limit Maximum number of comments to process with NLoN
#'   in one chunk. 0 means no limit.
#' @return The processed comments as a \code{data.table} with columns
#'   source, product, issue.id, comment.id, line.id, line.type, nchar
#'   and text.
#' @seealso ProcessApacheComments
#' @seealso ProcessMozillaComments
#' @export
ProcessRawComments <- function(comments, nlon.model=NULL, chunk.limit=0) {
  unknown <- setdiff(unique(comments$source), c("apache", "mozilla"))
  if (length(unknown)) {
    stop("Unknown source(s) %s", paste(unknown, collapse=", "))
  } else {
    comments <- rbind(ProcessApacheComments(comments[source == "apache"]),
                      ProcessMozillaComments(comments[source == "mozilla"]))
    comments[, text := gsub("[^[:graph:]]", " ", text)]
    if (!is.null(nlon.model)) {
      comments <- comments %>% ProcessText(NLoN, chunk.limit, nlon.model)
    }
    comments
  }
}

#' Process Apache Comments
#'
#' Processes Apache comments
#'
#' @param comments \code{data.table} object with comments.
#' @return Processed Apache comments.
ProcessApacheComments <- function(comments) {
  if (nrow(comments)) {
    comments$text <- strsplit(comments$text, "\n")
    comments[, {
      if (length(text[[1]])) {
        list(line.id=1:length(text[[1]]),
             line.type=LineType("author"),
             nchar=nchar(text), text=text[[1]])
      } else {
        list(line.id=as.integer(1),
             line.type=LineType("author"),
             nchar=as.integer(0), text="")
      }
    }, by=list(source, product, issue.id, comment.id)]
  }
}

#' Process Mozilla comments
#'
#' Processes Mozilla comments.
#'
#' @param comments \code{data.table} object with comments.
#' @return Processed Mozilla comments.
ProcessMozillaComments <- function(comments) {
  if (nrow(comments)) {
    comments$text <- strsplit(comments$text, "\n")
    comments$raw.text <- strsplit(comments$raw.text, "\n")
    comments[, {
      if (length(text[[1]])) {
        list(line.id=1:length(text[[1]]),
             line.type=ClassifyTextLines(text[[1]], raw.text[[1]]),
             nchar=nchar(text[[1]]),
             text=text[[1]])
      } else {
        list(line.id=as.integer(1), line.type=LineType("author"),
             nchar=as.integer(0), text="")
      }
    }, by=list(source, product, issue.id, comment.id)]
  }
}

#' Split Paragraphs
#'
#' Splits comments into paragraphs
#'
#' @param comments \code{data.table} object with comments.
#' @return \code{data.table} comments with columns source, product,
#'   issue.id, comments.id, paragraph.id, text and nchar.
#' @export
SplitParagraphs <- function(comments) {
  re.split <- ifelse(comments$source == "mozilla", "\n[[:space:]]*\n", "\n")
  comments$text <- strsplit(comments$text, re.split)
  comments[, {
    if (length(text[[1]])) {
      list(paragraph.id=1:length(text[[1]]),
           text=text[[1]],
           nchar=nchar(text[[1]]))
    } else {
      list(paragraph.id=as.integer(1), text="")
    }
  }, by=list(source, product, issue.id, comment.id)]
}

#' NL Comments
#'
#' Subset comments to only keep natural language comments from the
#' author and splits by paragraphs.
#'
#' @param comments \code{data.table} object with comments.
#' @return \code{data.table} comments with columns source, product,
#'   issue.id, comments.id, paragraph.id, text and nchar.
#' @export
NLComments <- function(comments) {
  if (any(comments[, grepl("[[:alpha:]]", text) & nlon == "NL" & line.type == "author"])) {
    comments <- comments[grepl("[[:alpha:]]", text) &
                         nlon == "NL" &
                         line.type == "author",
                         list(text=paste(text, collapse="\n")),
                         by=list(source, product, issue.id, comment.id)]
    comments[, text := CleanText(text)]
    comments <- comments[nchar(text) > 0] %>% SplitParagraphs
  } else unique(comments[, list(source, product)])
}

#' Split Sentences
#'
#' Split NL comments into sentences.
#'
#' @param comments \code{data.table} object with comments.
#' @return \code{data.table} comments with columns source, product,
#'   issue.id, comments.id, paragraph.id, sentence.id, text and nchar.
#' @export
SplitSentences <- function(comments) {
  logging::loginfo("Splitting %d text documents into sentences", nrow(comments))
  t <- system.time({
    comments[, text := tokenizers::tokenize_sentences(text)]
    comments <- comments[, if (length(text[[1]])) {
                             list(sentence.id=1:length(text[[1]]),
                                  text=gsub("\\s+", " ", text[[1]]),
                                  nchar=nchar(text[[1]]))
                           }, by=list(source, product, issue.id,
                                      comment.id, paragraph.id)]
  })
  logging::loginfo("%f seconds ellapsed", t[3])
  comments
}

#' Emoticons
#'
#' Identify emoticons
#'
#' @param comments \code{data.table} object with comments.
#' @return Emoticons.
#' @export
Emoticons <- function(comments) {
  setkey(comments, source, product, issue.id, comment.id, paragraph.id)
  emoticons <- EmoticonFindeR::EmoticonsAndEmojis(comments) %>% RemoveTextCols
  if (nrow(emoticons)) {
    emoticons[type != "slack"]
  } else unique(comments[, list(source, product)])
}

#' SentiStrength
#'
#' Runs SentiStrength on comments.
#'
#' @param comments \code{data.table} object with comments.
#' @param chunk.limit Maximum number of comments to process at the
#'   same time with SentiStrength.
#' @param prefix Prefix to add to the result's column names.
#' @return comments with added columns valence.min, valence.max,
#'   arousal.min and arousal.max.
#' @export
SentiStrength <- function(comments, chunk.limit=0, prefix=NULL) {
  ProcessText(comments, function(text) {
    arousal.data <- RSentiStrength::SentiStrengthData("tensidata_softeng")
    res <- cbind(RSentiStrength::SentiStrength(text),
                 RSentiStrength::SentiStrength(text, arousal.data))
    names <- c("valence.min", "valence.max",
               "arousal.min", "arousal.max")
    if (is.null(prefix)) {
      setnames(res, names)
    } else {
      setnames(res, paste(prefix, names, sep="."))
    }
  }, chunk.limit) %>% RemoveTextCols
}

#' Senti4SD
#'
#' Runs Senti4SD on comments.
#'
#' @param comments \code{data.table} object with comments.
#' @param model The LiblineaR model to use for Senti4SD.
#' @param memory.limit Maximum amount of memory (in GB) to use for one
#'   run of Senti4SD. Overrides \code{senti4sd.chunk.size} by setting
#'   it to \code{500 * senti4sd.memory.limit}.
#' @param chunk.limit Maximum number of comments to process at the
#'   same time with Senti4SD.
#' @return data.table as returned by \code{RSenti4SD::RunSenti4SD}.
#' @export
Senti4SD <- function(comments, model, memory.limit=0, chunk.limit=1000) {
  RSenti4SD::RunSenti4SD(comments, model, chunk.limit,
                         memory.limit) %>% RemoveTextCols
}
