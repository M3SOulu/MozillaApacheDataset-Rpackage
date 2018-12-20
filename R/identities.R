multi.and.re <- "[^[:alnum:]]and[^[:alnum:]]"
multi.extract.re <- "^([^<]+<[^>]+>)(.*[[:alnum:]].*)$"
email.re1 <- "^(.*)<([^<>]+)>.*$"
email.re2 <- "^([^[:blank:]]+@[^[:blank:]]+).*$"
email.re3 <- "^(.*)[[:blank:]]([^[:blank:]]+@[^[:blank:]]+).*$"

#' Split author
#'
#' Split author fields based on various regex.
#'
#' @param author.key The author fields.
#' @return A list of splitted authors.
SplitAuthor <- function(author.key) {
  if (grepl(multi.and.re, author.key)) {
    authors <- strsplit(author.key, multi.and.re)[[1]]
    unlist(lapply(authors, SplitAuthor))
  } else if (grepl(multi.extract.re, author.key)) {
    c(gsub(multi.extract.re, "\\1", author.key),
      SplitAuthor(gsub(multi.extract.re, "\\2", author.key)))
  } else author.key
}

#' Extract Email
#'
#' Extraction email from author fields.
#'
#' @param author The author fields.
#' @return The author fields as a \code{data.table} object with the
#'   author field, author name amd author email.
ExtractEmail <- function(author) {
  if (grepl(email.re1, author)) {
    ## print("1")
    data.table(author,
               author.name=sub(email.re1, "\\1", author),
               author.email=sub(email.re1, "\\2", author))
  } else if (grepl(email.re2, author)) {
    ## print("2")
    data.table(author,
               author.name=sub(email.re2, NA_character_, author),
               author.email=sub(email.re2, "\\1", author))

  } else if (grepl(email.re3, author)) {
    ## print("3")
    data.table(author,
               author.name=sub(email.re3, "\\1", author),
               author.email=sub(email.re3, "\\2", author))
  } else data.table(author, author.name=author, author.email=NA_character_)
}

#' Strip Non Alpha
#'
#' Strip non alpha numerical trailing characters from strings.
#'
#' @param s The character strings.
#' @return The character strings stripped.
StripNonAlpha <- function(s) {
  s <- gsub("^[^[:alnum:]]*|[^[:alnum:]]*$", "", s)
  gsub("[[:blank:]][[:blank:]]+", " ", s)
}

#' Git identities
#'
#' List identities from git logs.
#'
#' @param git.in Input RDS file.
#' @return \code{data.table} object with identities with source,
#'   author key, author field, author name, author email and number of
#'   commits.
GitIdentities <- function(git.in) {
  people.git <- readRDS(git.in)[, list(source, hash, author.key=author)]

  people.git[, multi.email := grepl("@.+@", author.key)]
  people.git[, multi.and := grepl(multi.and.re, author.key)]
  people.git[, multi.plus := grepl(", plus ", author.key)]

  people.git <- people.git[, list(author=SplitAuthor(author.key),
                                  N=length(unique(hash))),
                           by=c("source", "author.key")]
  people.git[, {
    parsed <- ExtractEmail(author)
    list(author.name=parsed$author.name,
         author.email=parsed$author.email,
         N=N)
  }, by=c("source", "author.key", "author")]
}

#' Identities
#'
#' List identities from git, jira and bugzilla logs.
#'
#' @param git.in Git input RDS file.
#' @param bugzilla.in Bugzilla input RDS file.
#' @param jira.in Jira input RDS file.
#' @param identities.commits.out Output file for git identities.
#' @param identities.bugs.out Output file for Jira and Bugzilla identities.
#' @param identities.out Output file for all identities.
Identities <- function(git.in, bugzilla.in, jira.in, identities.commits.out,
                       identities.bugs.out, identities.out) {
  people.bugzilla <- readRDS(bugzilla.in)[, .N, by=c("source", "author.email")]
  people.jira <- readRDS(jira.in)[, .N, by=c("source", "author.key",
                                             "author.name", "author.dname",
                                             "author.email")]
  people.git <- GitIdentities(git.in)

  people.jira[, author := author.dname]
  people.bugzilla[, author.key := author.email]
  people.bugzilla[, author := author.email]

  people <- rbind(people.git, people.bugzilla, people.jira, fill=TRUE)
  people[!grepl("[[:alpha:]]", author.name), author.name := NA_character_]
  people[is.na(author.dname), author.dname := author.name]
  people[, author.name := StripNonAlpha(tolower(author.name))]
  people[, author.dname := StripNonAlpha(tolower(author.dname))]
  people[, author.email := StripNonAlpha(tolower(author.email))]
  people[, id := 1:.N]

  fwrite(people[grepl("VCS", source)], identities.commits.out)
  fwrite(people[!grepl("VCS", source)], identities.bugs.out)
  fwrite(people, identities.out)
  invisible(NULL)
}

#' Parse Apache Email
#'
#' Converts Apache email addresses obfuscated with "dot" and "at".
#'
#' @param email Email address.
#' @return The email address with "dot" and "at" replaced by appropriate symbol.
ParseApacheEmail <- function(email) {
  gsub(" dot ", ".", gsub(" at ", "@", email))
}

#' Merge by name
#'
#' Merge identities by name.
#'
#' @param people Identities \code{data.table}.
#' @return Identities with the same merged.id for identities with the same name.
MergeByName <- function(people) {
  toignore <- c("", "disabled imported user", "unknown", "root",
                "daniel", "timeless", "andreas", "stefan", "david",
                "simon", "bryan", "jason", "sergey", "john", "ian",
                "steve", "martin", "tn", "alex", "mattn", "brendan",
                "andrew", "brian", "rico", "axel", "adam", "sid",
                "bob", "pablo", "andrey", "jordan", "dennis", "arun",
                "ming", "jim", "aleth", "alexander", "andrea", "eric",
                "rahul", "chris", "pavlo", "ryan", "victor", "phil",
                "philipp", "michael", "ajay", "nick", "marc", "doug",
                "wolf", "matt", "jaoki", "thomas", "vlad", "hdev",
                "ashish", "max", "julian", "admin", "ubuntu",
                "benedict", "manu", "jesse", "vladimir", "tony",
                "joe", "ben", "paul", "jay", "jan", "joshi", "josh",
                "anton", "jonathan", "kevin", "james", "darin",
                "anatole", "mike", "svn", "mona", "antonio",
                "cloud user", "alex", "david", "sergey",
                "liveandevil", "alberto", "alfredo", "anonymous",
                "anusha", "sandeep", "philip", "edward", "gilles",
                "vincent", "elliott", "pradeep", "poorna", "nirmal",
                "gnodet", "fengyu", "niclas", "sethah", "johann",
                "philippe")

  by.name <- people[!is.na(author.name) & !author.name %in% toignore]
  by.name <- by.name[author.name %in% by.name[, .N, by="author.name"][N > 1]$author.name]
  by.name <- by.name[grepl(" ", author.name) | nchar(author.name) > 5 |
                     !grepl("^[a-z0-9]+$", author.name)]
  by.name[, merged.id := min(id), by="author.name"]

  by.name <- rbind(rbindlist(lapply(grep("@.*=.*@", people$author.email), function(i) {
    p <- people[i]
    name <- sub("^.* = (\\w*) = .*$", "\\1", p$author.email)
    if (name %in% c("markt", "rgodfrey", "rfscholte", "olamy", "jgoodyear",
                    "sandy", "cnauroth", "lewismc", "quetwo", "elecharny",
                    "sebb", "oheger", "rmannibucau")) {
      mid <- by.name[author.name == p$author.name, min(merged.id, na.rm=TRUE)]
      res <- people[author.name == name]
      res[, merged.id := mid]
      res
    }
  })), by.name)

  tomerge <- list(c("ramangrover29", "ramangrover29@gmail.com"),
                  c("maaritlaine", "maarit"),
                  c("maxime beauchemin", "ldap/maxime_beauchemin"),
                  c("madhusudancs@gmail.com", "madhusudan.c.s", "madhusudancs"),
                  c("matt_sergeant", "matt sergeant"))
  rbind(rbindlist(lapply(tomerge, function(ids) {
    res <- people[author.name %in% ids]
    res[, merged.id := min(id)]
    res
  })), by.name)
}

#' Merge by email
#'
#' Merge identities by email.
#'
#' @param people Identities \code{data.table}.
#' @return Identities with the same merged.id for identities with the same email.
MergeByEmail <- function(people) {
  toignore <-c("", "none@none", "dev-null@apache.org", "bugzilla",
               "you@example.com", "unknown", "noreplay@example.com",
               "disabled@apache.org", "notifications@github.com",
               "unknown@apache.org")

  by.email <- people[!author.email %in% toignore & !is.na(author.email)]
  by.email <- by.email[author.email %in% by.email[, .N, by="author.email"][N > 1]$author.email]
  by.email[, merged.id := min(id), by="author.email"]

  tomerge <- list(c("ramangrover29@123451ca-8445-de46-9d55-352943316053",
                    "ramangrover29@gmail.com@eaa15691-b419-025a-1212-ee371bd00084",
                    "ramangrover29@gmail.com"),
                  c("kotikov.vladimir@gmail.com",
                    "v-vlkoti@microsoft.com"),
                  c("root@10-209-120-17.cloud.opsource.net",
                    "anthonyshaw@apache.org"),
                  c("jpercivall@apache.org",
                    "joepercivall@yahoo.com"),
                  c("pradeep.agrawal@freestoneinfotech.com",
                    "pradeep@apache.org"),
                  c("surkov.alexander@gmail.com", "surkov.alexander"),
                  c("matti@mversen.de", "bugzilla@mversen.de"),
                  c("mozilla@davedash.com", "mozilla+bugcloser@davedash.com"),
                  c("ben@bengoodger.com", "bugs@bengoodger.com", "ben@bengoodger.com"),
                  c("tbsaunde@tbsaunde.org", "tbsaunde+mozbugs@tbsaunde.org"),
                  c("phiw@l-c-n.com", "phiw2@l-c-n.com"),
                  c("jeff@jbalogh.me", "mozilla@jbalogh.me"),
                  c("tbsaunde@tbsaunde.org", "tbsaunde+mozbugs@tbsaunde.org"),
                  c("jwalden+bmo@mit.edu", "jwalden+fxhelp@mit.edu", "jwalden@mit.edu"),
                  ## c("terrence.d.cole@gmail.com", "tcole@mozilla.com"), ## Not sure
                  c("bugzillamozillaorg_serge_20140323@gautherie.fr", "sgautherie@free.fr"),
                  c("maxime.beauchemin@airbnb.com", "maximebeauchemin@gmail.com"),
                  c("liyang.gmt8@gmail.com", "liyang@apache.org"),
                  c("bigosmallm@gmail.com", "bigosmallm@apache.org"),
                  c("stack@duboce.net", "stack@apache.org"),
                  c("ajs6f@virginia.edu", "ajs6f@apache.org"),
                  c("tnine@apigee.com", "toddnine@apache.org", "todd.nine@gmail.com"),
                  c("tedyu@apache.org", "yuzhihong@gmail.com"),
                  c("peihe@google.com", "pei@apache.org"),
                  c("stephan.ewen@tu-berlin.de", "sewen@apache.org"),
                  c("ajs6f@apache.org", "ajs6f@virginia.edu"),
                  c("vasia@apache.org", "vasilikikalavri@gmail.com"),
                  c("mck@apache.org", "mick@semb.wever.org"),
                  c("eyal@apache.org", "eallweil@paypal.com"),
                  c("bigosmallm@gmail.com", "bigosmallm@apache.org"),
                  c("harbs@in-tools.com", "harbs@apache.org"),
                  c("u.celebi@fu-berlin.de", "uce@apache.org"),
                  c("mjsax@informatik.hu-berlin.de", "mjsax@apache.org"),
                  c("jiatuer@163.com", "zhongjian@apache.org", "jiazhong@apache.org", "jiazhong@ebay.com", "hellowowde110@gmail.com"),
                  c("vivekb.balakrishnan@gmail.com", "vivekb.balakrishnan"),
                  c("ziliu@linkedin.com", "ziyang.liu@asu.edu"),
                  c("andreas@continuuity.com", "anew@apache.org"),
                  c("btellier@linagora.com", "benwa@minet.net"),
                  c("th.kurz@gmail.com", "tkurz@apache.org"),
                  c("bernd_mozilla@gmx.de", "bmlk@gmx.de"),
                  c("mibo@mirb.de", "michael.bolz@sap.com"),
                  c("puru.shah@gmail.com", "purushah@yahoo-inc.com"),
                  c("dhruv.mahajan@gmail.com", "dhruv@apache.org"),
                  c("reka@wso2.com", "rthirunavukkarasu23@gmail.com"),
                  c("elliott.neil.clark@gmail.com", "eclark@apache.org"),
                  c("vincent.poon@salesforce.com", "vincentpoon@gmail.com"),
                  c("zhfengyu@corp.netease.com", "gzfengyu@corp.netease.com"),
                  c("poorna@cask.co", "poorna@apache.org"),
                  c("seth.hendrickson16@gmail.com", "shendrickson@cloudera.com"),
                  c("isuruh@wso2.com", "isuruh@apache.org"))

  rbind(rbindlist(lapply(tomerge, function(ids) {
    res <- people[author.email %in% ids]
    res[, merged.id := min(id)]
    res
  })), by.email)
}

#' Build merging graph
#'
#' Build an \code{igraph} object based on identities and merging.
#'
#' @param people List of identities as a \code{data.table} object.
#' @param merging Merging of people.
#' @return A graph with identities as nodes and merging as edges.
BuildMergingGraph <- function(people, merging) {
  g <- make_empty_graph(n=nrow(people), directed=FALSE)
  for (n in names(people)) {
    g <- set.vertex.attribute(g, n, value=people[[n]])
  }
  g + edges(t(merging))
}

#' Identity merging
#'
#' Make identity merging.
#'
#' @param identities.in Input RDS file of identities.
#' @param idmerging.out Output RDS file of identity merging.
IdentityMerging <- function(identities.in, idmerging.out) {
  people <- read.csv("data/identities.csv", stringsAsFactors=FALSE)
  people <- as.data.table(people)

  ## Apache fix
  people[source == "Apache", author.email := ParseApacheEmail(author.email)]
  people[grepl(" ", author.dname), author.name := author.dname]
  people[, author.email.domain := sub("^.*@", "", author.email)]

  by.name <- MergeByName(people)
  by.email <- MergeByEmail(people)

  merging <- rbind(by.email[, list(id, merged.id)], by.name[, list(id, merged.id)])
  g <- BuildMergingGraph(people, merging)

  people[, merged.id := as.integer(clusters(g)$membership)]
  people[, merged.id := min(id), by="merged.id"]

  fwrite(people, idmerging.out)
  invisible(NULL)
}

#' Mozilla developers
#'
#' Add identity merging to mozilla developers.
#'
#' @param idmerging.in Input RDS file of identity merging.
#' @param mozdev.in Input CSV file of mozilla developers.
#' @return Mozilla developers with merged.id.
MozillaDevelopers <- function(idmerging.in, mozdev.in) {
  idmerging <- fread(idmerging.in)
  mozdev <- fread(mozdev.in)
  mozdev$merged.id <- NULL
  merge(unique(idmerging[, list(author.email, merged.id)]),
        mozdev, by="author.email")
}
