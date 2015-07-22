var collect = require('collect-stream');

module.exports = function (db, key, cb) {
  var c = db.iterator({
    gt: 'l!' + key + '!',
    lt: 'l!' + key + '!~'
  });
  var prev = [];
  c.next(function f (err, key, value) {
    if (err) return cb(err);
    if (!key) return cb(null, prev);
    prev.push(key.toString().split('!')[2]);
    c.next(f);
  });
};
