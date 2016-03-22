Tail = require '../tail-forever.coffee'

options =
  "start":0
  "maxSize":52428800
  "bufferSize":1048576
  "encoding":"utf-8"
  # "maxLineSize" : 10

f = './test.log'
t = new Tail(f, options)

t.on 'line', (line) ->
  console.log  line
