var logseneToken = 'LOGSENE-APP-TOKEN-GOES-HERE'

var zlib = require('zlib')
var net = require('net')
var client = null
var connected = false
var start = 0
var errorCounter = 0
var Logagent = require('logagent-js')
var lp = new Logagent('./pattern.yml')
function connectLogsene (cbf) {
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
      var tid = setTimeout(function () {connectLogsene(cbf)}, 50)
      if (tid.unref) tid.unref()
    } else {
      console.log('More than 10 connection errors, exit()')
      process.exit(1)
    }
    connected = false
  })
  client.on('end', function () {
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
process.on('exit', disconnectLogsene)

function shipLogs (logObj, cbf) {
  var json = JSON.stringify(logObj)
  console.log('send to logsene ' + json)
  client.write(json + '\n')
  cbf()
}

function parseLogs (err, data) {
  //console.log('received for parsing' + JSON.stringify(data))
  if (!err || err == 'not found') {
    Object.keys(this.event).forEach(function (key) {
      if(key!=='message')
        data[key] = this.event[key]
    }.bind(this))
    Object.keys(this.result).forEach(function (key) {
      if (key !== 'logEvents') {
        data['meta_' + key] = this.result[key]
      }
    }.bind(this))
    shipLogs(data, function checkDone () {
      //console.log('I\'m at ' + this.index + ' of ' + this.size)
      if (this.index === this.size - 1) {
        this.context.succeed('Successfully processed ' + this.result.logEvents.length + ' log events.')
        disconnectLogsene()
      }
      this.index++
    }.bind(this))
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
      for (var i = 0; i < result.logEvents.length; i++) {
        result.logEvents[i]['logsene-app-token'] = logseneToken
        console.log('event ', i, ': ', JSON.stringify(result.logEvents[i]))
        lp.parseLine(result.logEvents[i].message, result.logGroup, 
          parseLogs.bind({ // binding parseLogs to this variables, accessible as this.context et. in parseLogs
            context: context, 
            result: result, 
            event: result.logEvents[i], 
            index: i, // required to check when finished
            size: result.logEvents.length})
        )
      }
      errorCounter = 0
    }
  })
  console.log(Date.now() - start)
}
function handler (event, context) {
  start = Date.now()
  if (!connected) {
    connectLogsene(function () {
      pushLogs(event, context)
    })
  } else {
    pushLogs(event, context)
  }
}
exports.handler = handler
