var demand = require("must")
var parse = require("..").parse

describe("SyslogProtocol", function() {
  describe(".parse", function() {
    it("must return null given a wrong format", function() {
      demand(parse("<15>")).be.null()
    })

    describe("given RFC 3164 with ISO 8601 timestamp", function() {
      ;[
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
      ].forEach(function(facility, code) {
        // http://tools.ietf.org/html/rfc3164#section-4.1.1
        it("must parse " + facility + " facility", function() {
          var priority = String(code * 8 + 6)
          var msg = "<"+priority+">1987-06-18T15:20:30.337Z server user: test"
          var syslog = parse(msg)
          syslog.severity.must.equal("info")
          syslog.severityCode.must.equal(6)
          syslog.facility.must.equal(facility)
          syslog.facilityCode.must.equal(code)
        })
      })

      ;[
        "emerg",
        "alert",
        "crit",
        "err",
        "warning",
        "notice",
        "info",
        "debug"
      ].forEach(function(severity, code) {
        // http://tools.ietf.org/html/rfc3164#section-4.1.1
        it("must parse " + severity + " severity", function() {
          var priority = String(1 * 8 + code)
          var msg = "<"+priority+">1987-06-18T06:20:30.337Z server user: test"
          var syslog = parse(msg)
          syslog.severity.must.equal(severity)
          syslog.severityCode.must.equal(code)
          syslog.facility.must.equal("user")
          syslog.facilityCode.must.equal(1)
        })
      })


      it("must parse timestamp given UTC offset", function() {
        var msg = "<15>1987-06-18T15:20:30.337Z server user: Test 123"
        var syslog = parse(msg)
        syslog.time.must.eql(new Date(Date.UTC(1987, 5, 18, 15, 20, 30, 337)))
      })

      it("must parse timestamp given time offset", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 server user: Test 123"
        var syslog = parse(msg)
        syslog.time.must.eql(new Date(Date.UTC(1987, 5, 18, 15, 20, 30, 337)))
      })

      it("must parse host", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 example.org user: Test 123"
        parse(msg).host.must.equal("example.org")
      })

      // http://tools.ietf.org/html/rfc3164#section-4.1.2
      it("must parse host given IPv4 address", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 127.0.0.1 user: Test 123"
        parse(msg).host.must.equal("127.0.0.1")
      })

      // http://tools.ietf.org/html/rfc3164#section-4.1.2
      it("must parse host given IPv6 address", function() {
        var ipv6 = "1080:0:0:0:8:800:200C:417A"
        var msg = "<15>1987-06-18T18:20:30.337+03:00 "+ipv6+" user: Test 123"
        parse(msg).host.must.equal(ipv6)
      })

      it("must parse process", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 server user: Test 123"
        parse(msg).process.must.equal("user")
      })

      it("must not set pid if not given", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 server user: Test 123"
        parse(msg).must.not.have.property("pid")
      })

      it("must set process and pid if pid empty", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 server user[]: Test 123"
        var syslog = parse(msg)
        syslog.process.must.equal("user")
        syslog.pid.must.equal("")
      })

      it("must parse process and pid given 0", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 server user[0]: Test 123"
        parse(msg).pid.must.equal(0)
      })

      // A naive implementation with parseInt might consider "0x" a number.
      it("must parse pid given 0x", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 server user[0x]: Test 123"
        parse(msg).pid.must.equal("0x")
      })

      it("must parse message", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 server user: Test 123"
        parse(msg).message.must.equal("Test 123")
      })

      it("must parse message without space after colon", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 server user:Test 123"
        var syslog = parse(msg)
        syslog.process.must.equal("user")
        syslog.message.must.equal("Test 123")
      })

      // http://tools.ietf.org/html/rfc3164#section-4.1.3
      it("must parse colon-less TAG", function() {
        var msg = "<15>2014-03-14T03:03:01.337+02:00 server user[42] Test 123"
        var syslog = parse(msg)
        syslog.process.must.equal("user")
        syslog.pid.must.equal(42)
        syslog.message.must.equal("Test 123")
      })

      it("must parse everything correctly", function() {
        var msg = "<15>1987-06-18T18:20:30.337+03:00 server user[42]: Test 123"
        parse(msg).must.eql({
          facility: "user",
          severity: "debug",
          facilityCode: 1,
          severityCode: 7,
          time: new Date(Date.UTC(1987, 5, 18, 15, 20, 30, 337)),
          host: "server",
          process: "user",
          pid: 42,
          message: "Test 123"
        })
      })
    })
  })
})
