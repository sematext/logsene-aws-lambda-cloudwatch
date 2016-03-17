jschardet = require 'jschardet'
fs = require 'fs'

c = fs.readFileSync '/var/tmp/uc-agent-1.0.0.tar.gz'
a = jschardet.detect c
console.log a