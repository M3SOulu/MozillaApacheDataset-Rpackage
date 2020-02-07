#' Raw Data
#'
#' Returns the list of raw data files (JSON files). The data must be
#' stored in files named raw/<type>/<source>/<repo>.json(.gz) or
#' raw/<type>/<source>/<repo>/<sub>.json(.gz).
#'
#' @param datadir Directory where the data is stored.
#' @return A \code{data.table} with columns type, source, repo and sub
#'   where type is the type of repository (e.g. git, jira or
#'   bugzilla), source is either Apache or Mozilla, repo is the name
#'   of the git repository or the jira/bugzilla product tag and sub is
#'   a unique id representing the file.
#' @export
RawData <- function(datadir) {
  file.re <- "^(.*)\\.json(\\.gz)?$"
  files <- dir(file.path(datadir, "raw"), pattern=file.re, recursive=TRUE)
  repos <- strsplit(sub(file.re, "\\1", files), "/")
  subset <- sapply(repos, length) %in% 3:4
  repos <- t(sapply(repos[subset], function(file) {
    if (length(file) == 3) c(file, "")
    else if (length(file) == 4) file
  }))
  repos <- as.data.table(repos, names=letters[1:4])
  setnames(repos, c("type", "source", "repo", "sub"))
  repos$gzip <- grepl("\\.gz$", files[subset])
  repos[, sub := as.numeric(sub)]
  repos
}

#' File
#'
#' File returns a filenames for a list of repositories.
#' @param types Vector of repository types to keep (e.g. git, jira,
#'   bugzilla).
#' @param repos List of repositories as a \code{data.table} as
#'   returned by \code{RawData}.
#' @param datadir Directory where the data is stored.
#' @param subdir Sub-directory inside \code{datadir}.
#' @param subname Added to the name of the data file.
#' @param ext Extension of the data file.
#' @param file.func If input or output, adds call to drake's
#'   \code{file_in} or \code{file_out}.
#' @export
File <- function(types, repos, datadir, subdir=".", subname="",
                 ext="parquet", file.func="none") {
  files <- repos[type %in% types, {
    f <- file.path(datadir, subdir, type, source, repo)
    if (!is.na(sub) && sub != "") f <- file.path(f, sub)
    if (subname != "") f <- sprintf("%s_%s", f, subname)
    f <- sprintf("%s.%s", f, ext)
    if (ext != "parquet" && gzip) sprintf("%s.gz", f) else f
  }, by=list(source, repo, sub, gzip)]$V1
  switch(file.func,
         input=substitute(file_in(f), list(f=files)),
         output=substitute(file_out(f), list(f=files)),
         files)
}

#' Read Parquet
#'
#' Reads a Parquet table from a file, converts it to a
#' \code{data.table} and replaces _ with . in the column names.
#'
#' @param filename Name of the Parquet file.
#' @return The \code{data.table} object.
#' @export
ReadParquet <- function(filename) {
  logging::loginfo("Reading %s", filename)
  df <- arrow::read_parquet(filename)
  df <- as.data.table(df)
  setnames(df, gsub("_", ".", names(df), fixed=TRUE))
  df
}

#' Read Parquet
#'
#' Writes a \code{data.table} to a Parquet file after replacing . with
#' _ in the column names.
#'
#' @param df The \code{data.table} object.
#' @param filename Name of the Parquet file.
#' @param compression Compression algorithm to use.
#' @export
WriteParquet <- function(df, filename, compression="gzip") {
  logging::loginfo("Writing %s", filename)
  dirname <- dirname(filename)
  if (!file.exists(dirname)) {
    dir.create(dirname, recursive=TRUE)
  }
  setnames(df, gsub(".", "_", names(df), fixed=TRUE))
  arrow::write_parquet(df, filename, compression=compression)
}
