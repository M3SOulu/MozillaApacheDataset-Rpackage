#' Read File
#'
#' Read a CSV file and returns it as a \code{data.table} object.
#'
#' @param filename The CSV file name.
#' @param use.fread Whether to (try) to use \code{data.table::fread} or not.
#' @return The CSV file as a \code{data.table} object.
ReadFile <- function(filename, use.fread=TRUE) {
  ReadCSV <- function(e) as.data.table(read.csv(filename, stringsAsFactors=FALSE))
  if (use.fread) {
    tryCatch(fread(cmd=NULL), error=ReadCSV)
  } else {
    ReadCSV(NULL)
  }
}

#' Parse Extension
#'
#' Parses the extension of a filename by taking into account multiple
#' successive extensions (such as tar.gz). Modified from
#' \code{tools::file_ext}.
#'
#' @param x The filename.
#' @return The file extension.
ParseExtension <- function(x) {
  pos <- regexpr("(\\.([[:alnum:]]+))+$", x)
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
RemoveURL <- function(text) gsub("https?://[[:graph:]]*", "", text)

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
