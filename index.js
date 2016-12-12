'use strict'
var logseneToken = 'LOGSENE-APP-TOKEN-GOES-HERE'
var zlib = require('zlib')
var net = require('net')
var client = null
var connected = false
var start = 0
var errorCounter = 0
var Logagent = require('@sematext/logagent')
var lp = null

function connectLogsene (context, cbf) {
  client = net.connect(12201, 'logsene-receiver-syslog.sematext.com', function () {
    connected = true
    console.log('connected to Logsene')
    cbf()
  })
  client.on('error', function (err) {
    errorCounter++
    connected = false
    console.log(err)
    if (errorCounter < 10) {
      var tid = setTimeout(function () { connectLogsene(context, cbf) }, 50)
      if (tid.unref) tid.unref()
    } else {
      context.fail(err)
      console.log('More than 10 connection errors, exit()')
      process.exit(1)
    }
    connected = false
  })
  client.on('end', function () {
    disconnectLogsene()
  })
  client.on('timeout', function () {
    disconnectLogsene()
  })
}
function disconnectLogsene () {
  try {
    if (client === null || !connected) {
      return
    }
    console.log('disconnected from Logsene')
    client.end()
    client.destroy()
    client = null
    connected = false
  } catch (err) {
    console.log('Disconnect Logsene: ' + err)
  }
}
process.on('beforeExit', disconnectLogsene)
process.on('SIGTERM', disconnectLogsene)
process.on('SIGQUIT', disconnectLogsene)

function shipLogs (logObj, cbf) {
  var json = JSON.stringify(logObj)
  // console.log('send to logsene ' + json)
  client.write(json + '\n')
  if (cbf) {
    cbf(null, json)
  }
}

function parseLogs (meta, err, data) {
  // console.log('received for parsing' + JSON.stringify(data))
  if (!err || err === 'not found') {
    Object.keys(meta.event).forEach(function (key) {
      if (key !== 'message') {
        data[key] = meta.event[key]
      }
    })
    // console.log('Now my data is ' + JSON.stringify(data))
    Object.keys(meta.result).forEach(function (key) {
      if (key !== 'logEvents') {
        data['meta_' + key] = meta.result[key]
      }
    })
    // console.log('Now my data is ' + JSON.stringify(data))
    shipLogs(data, console.log)
  } else {
    console.log('Ooops! Got this error: ' + err)
  }
}

function pushLogs (event, context) {
  var payload = new Buffer(event.awslogs.data, 'base64')
  zlib.gunzip(payload, function (e, result) {
    if (e) {
      context.fail(e)
    } else {
      result = JSON.parse(result.toString('utf8'))
      console.log('Decoded payload: ', JSON.stringify(result))
      for (let i = 0; i < result.logEvents.length; i++) {
        result.logEvents[i]['logsene-app-token'] = logseneToken
        let logMeta = { // binding parseLogs to this variables, accessible as this.context et. in parseLogs
          context: context,
          result: result,
          event: result.logEvents[i],
          size: result.logEvents.length}
        // console.log('event ', i, ': ', JSON.stringify(result.logEvents[i]))
        lp.parseLine(result.logEvents[i].message, result.logGroup,
          parseLogs.bind(null, logMeta)
        )
      }
      setImmediate(context.succeed)
      errorCounter = 0
    }
  })
  console.log(Date.now() - start)
}

function handler (event, context) {
  start = Date.now()
  if (!connected) {
    connectLogsene(context, function () {
      if (!lp) {
        lp = new Logagent('./pattern.yml', {}, function () {
          pushLogs(event, context)
        })
      } else {
        pushLogs(event, context)
      }
    })
  } else {
    if (!lp) {
      lp = new Logagent('./pattern.yml', {}, function () {
        pushLogs(event, context)
      })
    } else {
      pushLogs(event, context)
    }
  }
}
exports.handler = handler
