#' Parse Extension
#'
#' Parses the extension of a filename by taking into account multiple
#' successive extensions (such as tar.gz). Modified from
#' \code{tools::file_ext}.
#'
#' @param x The filename.
#' @return The file extension.
ParseExtension <- function(x) {
  pos <- regexpr("(\\.([[:alnum:]]+))$", x)
  ifelse(pos > -1L, substring(x, pos + 1L), "")
}

#' Parse time zone
#'
#' Parses a textual time zone as a number.
#'
#' @param tz The timezone formatted as +dddd or -dddd.
#' @return The timezone as a number.
ParseTZ <- function(tz) {
  if (any(!grepl("^[-+]\\d\\d\\d\\d$", tz))) {
    stop("Timezone is not of the format +hhmm or -hhmm")
  }
  as.numeric(substr(tz, 1, 3)) + as.numeric(substr(tz, 4, 5)) / 60
}

#' Local time
#'
#' Converts a time to local time based on a timezone
#'
#' @param time The time object
#' @param tz The timezone formatted as +dddd or -dddd.
#' @return The local time.
LocalTime <- function(time, tz) {
  time + as.difftime(ParseTZ(tz), units="hours")
}

#' Remove URL
#'
#' Removes URLs from text.
#'
#' @param text The text.
#' @return The text without URL.
RemoveURL <- function(text) gsub("https?://[[:graph:]]*", "<url>", text)

#' Clean Text
#'
#' Clean text by removing URLs, replacing non graphical symbols with
#' space and trailing white spaces.
#'
#' @param text The text.
#' @return The cleaned text.
CleanText <- function(text) {
  text <- RemoveURL(text)
  text <- str_replace_all(text,"[^[:graph:]\n]", " ")
  gsub("(^ +)|( +$)", "", text)
}

#' Remove NA Names
#'
#' Removes from a list columns which names are NA.
#'
#' @param l List.
#' @return The list without columns which names are NA.
RemoveNANames <- function(l) l[!is.na(names(l))]

#' Remove Columns
#'
#' Removes columns from a data.frame or list.
#'
#' @param df Data.frame or list.
#' @param cols Vector of columns.
#' @export
RemoveColumns <- function(df, cols) {
  for (col in cols) {
    df[[col]] <- NULL
  }
  df
}

#' Remove Text Columns
#'
#' Removes "text" and "raw.text" columns from a data.frame.
#' @param df Data.frame object.
#' @return The data.frame object without text and raw.text columns.
#' @export
RemoveTextCols <- function(df) {
  RemoveColumns(df, c("text", "raw.text"))
}

#' Subset columns
#'
#' Takes a subset of columns from a \code{data.table} objects. Doesn't
#' throw error if columns are missing.
#'
#' @param table \code{data.table} object.
#' @param cols Columns to subset.
#' @return The subset of the table.
#' @export
SubsetColumns <- function(table, cols) {
  cols <- cols[cols %in% names(table)]
  table[, cols, with=FALSE]
}
