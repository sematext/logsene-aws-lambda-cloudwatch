exports.parse = function(line) {
  var matches = RFC3164_WITH_ISO8601_TIME.exec(line)
  if (!matches) return null

  var priority = Number(matches[1])
  var facility = priority >> 3
  var severity = priority & 7

  var struct = {
    // RFC 3164 and RFC 5424 use the term "Numerical Code" for the integer
    // values of facilities and severities.
    facility: FACILITY[facility],
    facilityCode: facility,
    severity: SEVERITY[severity],
    severityCode: severity,

    // While RFC 3164 and RFC 5424 both call it TIMESTAMP, an instantiated
    // field would no longer be a stamp, but a time.
    time: new Date(matches[2]),

    // While RFC 3164 and RFC 5424 call it HOSTNAME, both allow IP addresses
    // to be used. Host is a more accurate name in that case.
    host: matches[3],

    // While RFC 3164 calls the identifier a TAG, it also mentions it being
    // used as process information:
    // http://tools.ietf.org/html/rfc3164#section-5.3
    // RFC 5424 calls it an APP-NAME field.
    //
    // For consistency with PID, use "process".
    process: matches[4],

    // While RFC 3164 calls it content, RFC 5424 calls it message.
    // http://tools.ietf.org/html/rfc5424#appendix-A.1
    // For consistency, stick to "message".
    message: matches[6]
  }

  // Both RFC 3164 and <syslog.h> talk about the value in brackets as PID.
  // http://tools.ietf.org/html/rfc3164#section-5.3
  //
  // It doesn't necessarily have to be a number.
  // As a random example, Heroku's TAG is heroku[router].
  var pid = matches[5]
  if (pid != null) struct.pid = pid && NUMBER.test(pid) ? Number(pid) : pid

  return struct
}

var RFC3164_WITH_ISO8601_TIME = new RegExp("^"
  + "<(\\d+)>"                         // Priority
  + "(\\d+-\\d+-\\d+T\\S+)"            // Time
  + " (\\S+)"                          // Host
  + " (?:(\\S+?)(?:\\[([^\\]]*)\\])?)" // Tag/Process + PID
  + "(?: |: ?)(.*)"                    // Message
)

// Facility names are set to match common <syslog.h> naming.
var FACILITY = exports.FACILITY = [
  "kern",
  "user",
  "mail",
  "daemon",
  "auth",
  "syslog",
  "lpr",
  "news",
  "uucp",
  "cron",
  "authpriv",
  "ftp",
  "ntp",
  "logaudit",
  "logalert",
  "clock",
  "local0",
  "local1",
  "local2",
  "local3",
  "local4",
  "local5",
  "local6",
  "local7"
]

// Severity names are set to match common <syslog.h> naming.
var SEVERITY = exports.SEVERITY = [
  "emerg",
  "alert",
  "crit",
  "err",
  "warning",
  "notice",
  "info",
  "debug"
]

var NUMBER = /^\d+$/
