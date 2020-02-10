multi.and.re <- "[^[:alnum:]]and[^[:alnum:]]"
multi.extract.re <- "^([^<]+<[^>]+>)(.*[[:alnum:]].*)$"
email.re1 <- "^(.*)<([^<>]+)>?.*$"
email.re2 <- "^([^[:blank:]]+@[^[:blank:]]+).*$"
email.re3 <- "^(.*)[[:blank:]]([^[:blank:]]+@[^[:blank:]]+).*$"

#' Split author
#'
#' Split Git author fields based on various regex.
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
#' Extracts email from Git author fields.
#'
#' @param person The fields.
#' @return The author fields as a \code{data.table} object with the
#'   author field, author name amd author email.
ExtractEmail <- function(person) {
  if (grepl(email.re1, person)) {
    data.table(person,
               person.name=sub(email.re1, "\\1", person),
               person.email=sub(email.re1, "\\2", person))
  } else if (grepl(email.re2, person)) {
    data.table(person,
               person.name=sub(email.re2, NA_character_, person),
               person.email=sub(email.re2, "\\1", person))

  } else if (grepl(email.re3, person)) {
    data.table(person,
               person.name=sub(email.re3, "\\1", person),
               person.email=sub(email.re3, "\\2", person))
  } else data.table(person, person.name=person, person.email=NA_character_)
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

#' Commits Identities
#'
#' Extract different author and committer identities from a set of commits.
#'
#' @param commits \code{data.table} commit log.
#' @return \code{data.table} with columns source, repo, key,
#'   displayname, type, name, email and N (number of commits).
#' @export
CommitsIdentities <- function(commits) {
  ids <- commits[, list(key=c(author, committer)),
                 by=list(source, repo, hash)]

  ids[, multi.email := grepl("@.+@", key)]
  ids[, multi.and := grepl(multi.and.re, key)]
  ids[, multi.plus := grepl(", plus ", key)]

  ids <- ids[, list(displayname=SplitAuthor(key),
                    N=length(unique(hash))),
             by=c("source", "repo", "key")]
  ids[, {
    with(ExtractEmail(displayname),
         list(type="commits",
              name=person.name,
              email=person.email,
              N=N))
  }, by=c("source", "repo", "key", "displayname")]
}

#' Issues Identities
#'
#' Extract different identities from a set of issues.
#'
#' @param issues \code{data.table} with issue log.
#' @return \code{data.table} with columns source, repo, key,
#'   displayname, type, name, email and N (number of commits).
#' @export
IssuesIdentities <- function(issues) {
  ids <- issues[, list(key=c(reporter.key, creator.key),
                       name=c(reporter.name, creator.name),
                       displayname=c(reporter.displayname, creator.displayname),
                       email=c(reporter.email, creator.email)),
                by=source]
  ids <- ids[!is.na(key) | !is.na(name) |
             !is.na(displayname) | !is.na(email)]
  ids$type <- "issues"
  ids$repo <- NA
  ids[, .N, by=list(type, source, repo, key, name, displayname, email)]
}

#' Issue Comments Identities
#'
#' Extract different identities from a set of issue comments.
#'
#' @param comments \code{data.table} with issue comment log.
#' @return \code{data.table} with columns source, repo, key,
#'   displayname, type, name, email and N (number of comments).
#' @export
IssueCommentsIdentities <- function(comments) {
  ids <- comments[, list(key=c(update.author.key, author.key),
                         name=c(update.author.name, author.name),
                         displayname=c(update.author.displayname,
                                       author.displayname),
                         email=c(update.author.email, author.email)),
                  by=source]
  ids <- ids[!is.na(key) | !is.na(name) |
             !is.na(displayname) | !is.na(email)]
  ids$type <- "issues"
  ids$repo <- NA
  ids[, .N, by=list(type, source, repo, key, name, displayname, email)]
}

#' Identities
#'
#' Extract identities from commits, issues and issue comments.
#'
#' @param commits \code{data.table} with Git commit identities.
#' @param issues \code{data.table} with issue identities.
#' @param comments \code{data.table} with issue comment
#'   identities.
#' @param file.out Parquet output file.
#' @return The \code{data.table} of identities if \code{file.out} is
#'   NULL, or the output filename otherwise.
#' @export
Identities <- function(commits, issues, comments, file.out=NULL) {
  people <- rbind(commits, issues, comments, fill=TRUE)
  people[!grepl("[[:alpha:]]", name), name := NA_character_]
  people[is.na(displayname), displayname := name]
  people[, name := StripNonAlpha(tolower(name))]
  people[, displayname := StripNonAlpha(tolower(displayname))]
  people[, email := StripNonAlpha(tolower(email))]
  ## people[is.na(key), key := email] ## TODO Remove after re-processing
  people[, id := 1:.N]
  if (!is.null(file.out)) {
    WriteParquet(people, file.out)
  }
  people
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
                "philippe", "david smith", "john doe", "paul smith",
                "steve smith", "john smith", "mike smith",
                "chris smith", "chris jones", "real name",
                "please ignore this troll (account disabled",
                "deleted user", "john lee", "michael smith")

  sub <- c("markt", "rgodfrey", "rfscholte", "olamy", "jgoodyear",
           "sandy", "cnauroth", "lewismc", "quetwo", "elecharny",
           "sebb", "oheger", "rmannibucau")
  people[grep("@.*=.*@", email)]$name
  people[grepl("@.*=.*@", email) &
         sub("^.* = (\\w*) = .*$", "\\1", email) %in% sub,
         name := sub("^.* = (\\w*) = .*$", "\\1", email)]
  people[grep("@.*=.*@", email)]$name

  by.name <- people[!is.na(name) & !name %in% toignore]
  sub <- by.name[, if ("commits" %in% type) list(N=.N), by="name"]
  by.name <- by.name[name %in% sub[N > 1]$name]
  by.name <- by.name[grepl(" ", name) | nchar(name) > 5 |
                     !grepl("^[a-z0-9]+$", name)]
  by.name[, merged.id := min(id), by="name"]

  tomerge <- list(c("ramangrover29", "ramangrover29@gmail.com"),
                  c("david philip brondsema", "dave brondsema"),
                  c("semen boikov", "sboikov"),
                  c("sheng wu", "wu sheng"),
                  c("vladimir ozerov", "vozerov"),
                  c("par niclas hedhman", "niclas hedhman"),
                  c("shao feng shi", "shaofeng shi"),
                  c("filip maj", "fil maj"),
                  c("samuel james corbett", "sam corbett"),
                  c("timothy a. bish", "timothy bish"),
                  c("nikolas wellnhofer", "nick wellnhofer"),
                  c("lburgazzoli", "luca burgazzoli"),
                  c("christian m\u00FCller", "christian mueller"),
                  c("antonenko alexander", "alexander antonenko"),
                  c("joey robert bowser", "joe bowser"),
                  c("xing zhang", "acton393"),
                  c("maxime beauchemin", "ldap/maxime_beauchemin"),
                  c("madhusudancs@gmail.com", "madhusudan.c.s", "madhusudancs"),
                  c("matt_sergeant", "matt sergeant"))
  rbind(rbindlist(lapply(tomerge, function(ids) {
    res <- people[name %in% ids]
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
               "noreply@example.com", "disabled@apache.org",
               "notifications@github.com", "unknown@apache.org",
               "noreply@github.com")

  by.email <- people[!email %in% toignore & !is.na(email)]
  by.email <- by.email[email %in% by.email[, .N, by="email"][N > 1]$email]
  by.email[, merged.id := min(id), by="email"]

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
                  c("bugzillamozillaorg_serge_20140323@gautherie.fr", "sgautherie@free.fr"),
                  c("maxime.beauchemin@airbnb.com", "maximebeauchemin@gmail.com"),
                  c("liyang.gmt8@gmail.com", "liyang@apache.org"),
                  c("bigosmallm@gmail.com", "bigosmallm@apache.org"),
                  c("stack@duboce.net", "stack@apache.org"),
                  c("hyatt@netscape.com", "hyatt@mozilla.org"),
                  c("mscott@netscape.com", "mscott@mozilla.org",
                    "scott@scott-macgregor.org"),
                  c("waterson@maubi.net", "waterson@netscape.com"),
                  c("mkaply@us.ibm.com", "mkaply@mozilla.com"),
                  c("pinkerton@netscape.com", "mikepinkerton@mac.com"),
                  c("blakeross@telocity.com", "bugzilla@blakeross.com"),
                  c("bienvenu@netscape.com", "bienvenu@nventure.com"),
                  c("ajs6f@virginia.edu", "ajs6f@apache.org"),
                  c("tnine@apigee.com", "toddnine@apache.org",
                    "todd.nine@gmail.com"),
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
                  c("jiatuer@163.com", "zhongjian@apache.org",
                    "jiazhong@apache.org", "jiazhong@ebay.com",
                    "hellowowde110@gmail.com"),
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
                  c("sfraser_bugs@smfr.org", "sfraser@netscape.com"),
                  c("mcafee@netscape.com", "mcafee@gmail.com"),
                  c("wtc@netscape.com", "wtc@google.com", "wtchang@redhat.com"),
                  c("mozilla@noorenberghe.ca", "mozilla@noorenberghe.ca"),
                  c("wjohnston@mozilla.org", "wjohnston2000@gmail.com"),
                  c("terrence@mozilla.com", "terrence.d.cole@gmail.com"),
                  c("cbiesinger@web.de", "cbiesinger@gmail.com", "cbiesinger@gmx.at"),
                  c("roc+@cs.cmu.edu", "roc@ocallahan.org"),
                  c("jones.chris.g@gmail.com", "cjones.bugs@gmail.com", "jones.chris.g@gmail.com"),
                  c("dougt@netscape.com", "dougt@dougt.org"),
                  c("seth@mozilla.com", "seth.bugzilla@blackhail.net"),
                  c("blassey@mozilla.com", "lassey@chromium.org", "brad@lassey.us"),
                  c("warren@netscape.com", "warrensomebody@gmail.com"),
                  c("rjc@netscape.com", "mozilla@rjcdb.com"),
                  c("cmanske@netscape.com", "cmanske@jivamedia.com"),
                  c("dbaron@fas.harvard.edu", "dbaron@dbaron.org"),
                  c("mwu@mozilla.com", "mwu.code@gmail.com"),
                  c("bryner@brianryner.com", "bryner@netscape.com",
                    "bryner@gmail.com", "bryner@uiuc.edu"),
                  c("briano@netscape.com", "briano@bluemartini.com"),
                  c("brade@netscape.com", "brade@comcast.net"),
                  c("darin@meer.net", "darin@netscape.com", "darin.moz@gmail.com"),
                  c("seth.hendrickson16@gmail.com", "shendrickson@cloudera.com"),
                  c("isuruh@wso2.com", "isuruh@apache.org"),
                  c("archaeopteryx@coole-files.de",
                    "aryx.bugmail@gmx-topmail.de"),
                  c("kats@mozilla.com", "bugmail@mozilla.staktrace.com"),
                  c("jst@mozilla.org", "jstenback+bmo@gmail.com",
                    "jst@mozilla.jstenback.com", "jst@jstenback.com"),
                  c("tyler.downer@gmail.com",
                    "tyler@christianlink.us", "tdowner@mozilla.com"),
                  c("mkanat@kerio.com", "mkanat@bugzilla.org"),
                  c("jlong@mozilla.com", "longster@gmail.com"),
                  c("kartikgupta0909@gmail.com", "kgupta@mozilla.com"),
                  c("n.nethercote@gmail.com", "nnethercote@mozilla.com"),
                  c("gijskruitbosch@gmail.com", "gijskruitbosch+bugs@gmail.com"),
                  c("dao@mozilla.com", "dao+bmo@mozilla.com"),
                  c("neil@parkwaycc.co.uk", "neil@httl.net"),
                  c("dholbert@cs.stanford.edu", "dholbert@mozilla.com"),
                  c("sspitzer@netscape.com", "sspitzer@mozilla.org", "seth@sspitzer.org"),
                  c("mwoodrow@mozilla.com", "matt.woodrow@gmail.com"),
                  c("maglione.k@gmail.com", "kmaglione+bmo@mozilla.com"),
                  c("alecf@netscape.com", "alecf@flett.org"),
                  c("bugzilla@standard8.plus.com", "standard8@mozilla.com"),
                  c("olli.pettay@helsinki.fi", "bugs@pettay.fi"))

  rbind(rbindlist(lapply(tomerge, function(ids) {
    res <- people[email %in% ids]
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

#' Find bots
#'
#' Find bots in list of merged identities.
#'
#' @param identities List of merged identities.
#' @return The log with an added boolean column is.bot.
FindBots <- function(identities) {
  bots <- list(emails=c("intermittent-bug-filer@mozilla.bugs",
                        "pulsebot@bots.tld",
                        "tbplbot@gmail.com",
                        "orangefactor@bots.tld",
                        "wptsync@mozilla.bugs",
                        "wptsync@mozilla.com",
                        "release-mgmt-account-bot@mozilla.tld",
                        "bug-husbandry-bot@mozilla.bugs",
                        "release+b2gbumper@mozilla.com",
                        "release+gaiajson@mozilla.com",
                        "release+l10nbumper@mozilla.com",
                        "release+treescript@mozilla.org",
                        "builds@apache.org",
                        "mxnet-ci",
                        "impala-public-jenkins@cloudera.com",
                        "bugzilla@gtalbot.org"),
               names=c("no author", "github"))
  bots <- identities[email %in% bots$emails | name %in% bots$names,
                     unique(merged.id)]
  identities[, is.bot := merged.id %in% bots]
  identities
}

#' Identity merging
#'
#' Makes identity merging.
#'
#' @param people \code{data.table} with identities to merge.
#' @param file.out Output Parquet file.
#' @return Identity merging \code{data.table} object.
#' @export
IdentityMerging <- function(people, file.out=NULL) {
  people <- copy(people)
  people[, email := gsub("%", "@", email)]
  people[, email.domain := sub("^.*@", "", email)]

  people[source == "apache", email := ParseApacheEmail(email)]
  people[source == "apache" & type == "issues" & grepl(" ", displayname), name := displayname]
  people[grepl("@formerly-netscape.com.tld$", email),
         email := sub("@formerly-netscape.com.tld$", "@netscape.com", email)]
  people[is.na(email) & grepl("^[^@<> ]+@[^@<> ]+$", key), email := key]

  by.name <- MergeByName(people)
  by.email <- MergeByEmail(people)
  by.key <- people[is.na(repo),
                   if (.N > 1) list(id=id, merged.id=min(id)),
                   by=list(source, key)]

  merging <- rbind(by.email[, list(id, merged.id)],
                   by.name[, list(id, merged.id)],
                   by.key[, list(id, merged.id)])
  g <- BuildMergingGraph(people, merging)

  people[, merged.id := as.integer(clusters(g)$membership)]
  people[, merged.id := min(id), by="merged.id"]
  people <- FindBots(people)

  if (!is.null(file.out)) {
    WriteParquet(people, file.out)
  }

  people
}
