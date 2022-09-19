events = require("events")
fs =require('fs')
async = require 'uclogs-async'
jschardet = require 'jschardet'
iconv = require('iconv-lite')
assert = require 'assert'
us = require 'underscore'

environment = process.env['NODE_ENV'] || 'development'

split = (size, chunk_size) ->
  result = []
  while size > 0
    if size >= chunk_size
      result.push chunk_size
      size -= chunk_size
    else
      result.push size
      size = 0
  return result

class SeriesQueue

  next : () ->
    if @queue.length >= 1 && not @lock
      element = @queue.shift()
      @lock = true # acqure lock
      @task(element, () =>
        @lock = false ## release lock
        if @queue.length >= 1
          setImmediate(() => @next() )
      )

  constructor:(@task) ->
    @queue = []
    @lock = false

  push: (element) ->
    @queue.push element

    setImmediate(() =>
      @next()
    )

  clean: () ->
    @queue = []

  length : () ->
    @queue.length


class Tail extends events.EventEmitter

  _readBlock:(block, callback) =>
    fs.fstat block.fd, (err, stat) =>
      if err
        return callback()

      start = @bookmarks[block.fd]
      end  = stat.size
      if start > end
        start = 0

      size = end - start

      if @maxSize > 0 and size > @maxSize
        start = end - @maxSize
        size = @maxSize
      if start < 0 
        start = 0
      
      split_size = if @bufferSize > 0 then @bufferSize else size
      async.reduce split(size, split_size), start, (start, size, callback) =>
        buff = Buffer.alloc(size)
        fs.read block.fd, buff, 0, size, start, (err, bytesRead, buff) =>
          if err
            @emit('error', err)
            return callback(err)

          if @encoding != 'auto'
            encoding = @encoding
          else
            detected_enc = jschardet.detect buff
            if not detected_enc?.encoding or detected_enc.confidence < 0.98
              encoding = "utf-8"
            else if not iconv.encodingExists detected_enc.encoding
              console.error "auto detected #{detected_enc.encoding} is not supported, use UTF-8 as alternative"
              encoding = 'utf-8'
            else
              encoding = detected_enc.encoding

          data = iconv.decode buff, encoding
          @buffer += data
          parts = @buffer.split(@separator)
          @buffer = parts.pop()
          @emit("line", chunk) for chunk in parts
          if @buffer.length > @maxLineSize
            @buffer = ''
          @bookmarks[block.fd] = start + bytesRead
          callback(null, @bookmarks[block.fd])
      , (err) =>
          if err 
            console.log err 
            return callback(err) if err
          else 
            return callback()

  _close: (fd) ->
    try 
      fs.closeSync fd
      if process.env.DEBUG == 'tail-forever' 
        console.log "\t\tfile closed " + fd
    catch err
      console.log err 
    finally
      @fileOpen = false
      @current.fd = null
      delete @bookmarks[fd]

  _checkOpen : (start, inode) ->
    ###
      try to open file
      start: the postion to read file start from. default is file's tail position
      inode: if this parameters present, the start take effect if only file has same inode
    ###
    try
      if @fileOpen 
        console.log 'file already open'
        return
      stat = fs.statSync @filename
      if not stat.isFile()
        throw new Error("#{@filename} is not a regular file")
      fd = fs.openSync(@filename, 'r')
      @fileOpen = true
      if process.env.DEBUG == 'tail-forever' 
        console.log "\t\t## FD open = " + fd    
      stat = fs.fstatSync(fd)
      @current = {fd: fd, inode: stat.ino}
      if start? and start >=0 and ( !inode or inode == stat.ino )
        @bookmarks[fd] = start
      else
        @bookmarks[fd] = stat.size

      @queue.push({type:'read', fd: @current.fd, inode: @current.inode})
    catch e
      if e.code == 'ENOENT'  # file not exists
        @current = {fd: null, inode: 0}
      else
        throw new Error("failed to read file #{@filename}: #{e.message}")


  ###
  options:
    - separator: default is '\n'
    - start: where start from, default is the tail of file
    - inode: the tail file's inode, if file's inode not equal this will treat a new file
    - interval: the interval millseconds to polling file state. default is 1 seconds
    - maxSize: the maximum byte size to read one time. 0 or nagative is unlimit.
    - maxLineSize: the maximum byte of one line
    - bufferSize: the memory buffer size. default is 1M. Tail read file content into buffer first. nagative value is no buffer
    - encoding: the file encoding. defalut value is "utf-8",  if "auto" encoding will be auto detected by jschardet
  ###
  constructor:(@filename,  @options = {}) ->
    super()
    assert.ok us.isNumber(@options.start), "start should be number" if @options.start?
    assert.ok us.isNumber(@options.inode), "inode should be number" if @options.inode?
    assert.ok us.isNumber(@options.interval), "interval should be number" if @options.interval?
    assert.ok us.isNumber(@options.maxSize), "maxSize should be number" if @options.maxSize?
    assert.ok us.isNumber(@options.maxLineSize), "start maxLineSize should be number" if @options.maxLineSize?
    assert.ok us.isNumber(@options.bufferSize), "bufferSize should be number" if @options.bufferSize?
    @fileOpen = false
    @separator = @options?.separator? || '\n'
    @buffer = ''
    @queue = new SeriesQueue(@_readBlock)
    @isWatching = false
    @bookmarks = {}
    @_checkOpen(@options.start, @options.inode)
    @interval = @options.interval ? 1000
    @maxSize = @options.maxSize ? -1
    @maxLineSize = @options.maxLineSize ? 1024 * 1024 # 1M
    @bufferSize = @options.bufferSize ? 1024 * 1024 # 1M
    @encoding = @options.encoding ? 'utf-8'
    if @encoding != 'auto' and not iconv.encodingExists @encoding
      throw new Error("#{@encoding} is not supported, check encoding supported list in https://github.com/ashtuchkin/iconv-lite/wiki/Supported-Encodings")
    @watch()


  watch: ->
    return if @isWatching
    @isWatching = true
    fs.watchFile @filename, {interval: @interval}, (curr, prev) => @_watchFileEvent curr, prev


  _watchFileEvent: (curr, prev) ->
    if (curr.ino != @current.inode) || curr.ino == 0
      ## file was rotate or relink
      ## need to close old file descriptor and open new one
      if @current && @current.fd
        # _checkOpen closes old file descriptor
        if process.env.DEBUG == 'tail-forever' 
          console.log "\t\tinode changed: @current.fd=" + @current.fd + " -> closing FD " + @current.fd
        oldFd = @current.fd
        @_close(oldFd)
        # @_checkOpen(0)
    if !@fileOpen 
      @_checkOpen(0)
    else
      @queue.push({type:'read', fd: @current.fd, inode: @current.inode})

  where : () ->
    if not @current.fd
      return null
    return {inode: @current.inode, pos: @bookmarks[@current.fd]}

  unwatch: ->
    @queue.clean()
    fs.unwatchFile(@filename)
    @isWatching = false
    if @current.fd
      memory = {inode: @current.inode, pos: @bookmarks[@current.fd]}
    else
      memory = {inode: 0, pos: 0}

    for fd, pos of @bookmarks
      @_close(parseInt(fd))
    @bookmarks = {}
    @current = {fd:null, inode:0}
    return memory


module.exports = Tail
