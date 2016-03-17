tailF
=====

a node.js library to implement tail -F. The power than tail -F is: it can watch a not exist file, also can continue watch file after file was removed.

# install
```bash
npm install tail-forever
```

# Use
## simple case
```javascript
Tail = require('tail-forever');

tail = new Tail("fileToTail");
tail.on("line", function(line) {
  console.log(line);
});
tail.on("error", function(error) {
  console.log('ERROR: ', error);
});
````

## save postion can continue watch next time
if you applicaiton down or have to restart, you want to continue watch file from last postion to avoid lost data during down time. you can do it like this:

```javascript
// unwatch return opsition which has two attributes: inode and pos. inode is the file's inode and pos is position where watched. you can save it on persistent file or database.
position = tail.unwatch()

// watch file after application restart. 
new Tail('/file/path', {start: position.pos, inode: position.inode})

```
