SyslogProtocol.js
========
[![NPM version][npm-badge]](http://badge.fury.io/js/syslog-protocol)
[npm-badge]: https://badge.fury.io/js/syslog-protocol.png

SyslogProtocol.js is a Syslog ([RFC 3164][rfc3164]) format parser that supports
high-precision timestamps ([RFC 3339][rfc3339], [ISO 8601][iso8601]).

Given a Syslog message with a high-precision timestamp:
```
<38>1987-06-18T15:20:30.337Z server sshd[42]: Accepted publickey for user
```

It'll return the following object (with `time` being an instance of `Date`):
```javascript
{ facility: "auth",
  facilityCode: 4,
  severity: "info",
  severityCode: 6,
  time: new Date("1987-06-18T15:20:30.337Z"),
  host: "server",
  process: "sshd",
  pid: 42,
  message: "Accepted publickey for user" }
```

[rfc3164]: https://tools.ietf.org/html/rfc3164
[rfc3339]: https://tools.ietf.org/html/rfc3339
[iso8601]: https://en.wikipedia.org/wiki/ISO_8601

Ironically, SyslogProtocol.js does *not* support plain [RFC 3164's
timestamps](https://tools.ietf.org/html/rfc3164#section-4.1.2), which are in
who-knows-what time zone and lack a year part. If you can, don't use them. If
you're really keen on them, please let me know and I'll see about implementing.

### Tour
- Supports [RFC 3164](rfc3164) with high-precision timestamps ([RFC
  3339][rfc3339], [ISO 8601][iso8601]).  
  For example, Rsyslog's `RSYSLOG_ForwardFormat` uses those.
- Supports colon-less TAG/process identifiers/messages (which Heroku log drains
  send).
- Facility and severity names are [`<syslog.h>`][syslog.h] and
  [`syslog(3)`][syslog(3)] compatible.
- All property names of the returned object have gone through serious sincere
  consideration and are amazingly well chosen.


Installing
----------
```
npm install syslog-protocol
```


Using
-----
Just require SyslogProtocol.js and use its `parse` function:
```javascript
var SyslogProtocol = require("syslog-protocol")
var msg = "<38>1987-06-18T15:20:30.337Z server sshd[42]: Accepted publickey"
SyslogProtocol.parse(msg)
```

### Alphanumeric process identifiers
SyslogProtocol.js can also handle alphanumeric process identifiers (`sshd[foo]`).
For example, given Heroku's forwarded log:
```
<158>1987-06-18T15:20:30.337Z d.550e8400-e29b-41d4-a716-446655440000 heroku[router] at=info method=GET path=/
```

SyslogProtocol.js will return:
```javascript
{ facility: "local3",
  facilityCode: 19,
  severity: "info",
  severityCode: 6,
  time: new Date("1987-06-18T15:20:30.337Z"),
  host: "d.550e8400-e29b-41d4-a716-446655440000",
  process: "heroku",
  pid: "router",
  message: "at=info method=GET path=/" }
```

### Properties
The returned object from `parse` has the following properties:

Property     | Description
-------------|---------
facility     | Facility name. See below for a [full list of facilities](#facilities).
facilityCode | Facility numeric code.
severity     | Severity name. See below for a [full list of severities](#severities).
severityCode | Severity numeric code.
time         | `Date` instance from the timestamp.
host         | Hostname or IP address.
process      | Process name.
pid          | Process identifier (taken from brackets after process name).<br> If the message lacks one, `pid` won't be set at all.<br> If it looks like a number, it'll be cast to `Number`.
message      | Rest of the message.

### Facilities
Facility names returned by SyslogProtocol.js match [`<syslog.h>`][syslog.h] and
[`syslog(3)`][syslog(3)].

Code | Facility
-----|---------
   0 | kern
   1 | user
   2 | mail
   3 | daemon
   4 | auth
   5 | syslog
   6 | lpr
   7 | news
   8 | uucp
   9 | cron
  10 | authpriv
  11 | ftp
  12 | ntp
  13 | logaudit
  14 | logalert
  15 | clock
  16 | local0
  17 | local1
  18 | local2
  19 | local3
  20 | local4
  21 | local5
  22 | local6
  23 | local7

### Severities
Severity names returned by SyslogProtocol.js match [`<syslog.h>`][syslog.h] and
[`syslog(3)`][syslog(3)].  
Blame them for the inconsistent naming.

Code | Severity
-----|---------
   0 | emerg
   1 | alert
   2 | crit
   3 | err
   4 | warning
   5 | notice
   6 | info
   7 | debug

[syslog.h]: http://pubs.opengroup.org/onlinepubs/7908799/xsh/syslog.h.html
[syslog(3)]: http://linux.die.net/man/3/syslog

License
-------
SyslogProtocol.js is released under a *Lesser GNU Affero General Public License*, which
in summary means:

- You **can** use this program for **no cost**.
- You **can** use this program for **both personal and commercial reasons**.
- You **do not have to share your own program's code** which uses this program.
- You **have to share modifications** (e.g. bug-fixes) you've made to this
  program.

For more convoluted language, see the `LICENSE` file.


About
-----
**[Andri Möll](http://themoll.com)** typed this and the code.  
[Monday Calendar](https://mondayapp.com) supported the engineering work.

If you find SyslogProtocol.js needs improving, please don't hesitate to type to me now
at [andri@dot.ee][email] or [create an issue online][issues].

[email]: mailto:andri@dot.ee
[issues]: https://github.com/moll/js-syslog-protocol/issues
