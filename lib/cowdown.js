var collect = require('collect-stream');
var inherits = require('inherits');
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN;
var prefix = require('./prefix.js');
var xtend = require('xtend');
var notFound = require('./not_found.js');

module.exports = Cow;
inherits(Cow, AbstractLevelDOWN);

function Cow (db, key) {
  if (!(this instanceof Cow)) return new Cow(db, key);
  this._db = db;
  this._key = key;
}

Cow.prototype._open = function (opts, cb) {
  var self = this;
  process.nextTick(function () { cb(null, self) });
};

Cow.prototype._prefix = function (pre, key) {
  return prefix(pre, this._key, key);
};

Cow.prototype._xget = function (pkey, key, opts, cb) {
  var self = this;
  self._db.get(prefix('d', pkey, key), opts, function (err, value) {
    if (err && notFound(err)) get()
    else if (err) cb(err)
    else cb(new Error('not found'), undefined, true)
  });
  function get () {
    self._db.get(prefix('n', pkey, key), opts, cb);
  }
};

Cow.prototype._get = function (key, opts, cb) {
  var self = this;
  self._xget(self._key, key, opts, onxget);
  
  function onxget (err, value, deleted) {
    if (deleted) cb(err)
    else if (err && notFound(err)) {
      checkp(self._key, cb)
    }
    else cb(err, value)
  }
  function checkp (pkey, cb) {
    self._getPrev(pkey, function (err, prev) {
      if (err) return cb(err)
      if (prev.length === 0) return cb(new Error('not found'))
      
      (function next () {
        if (prev.length === 0) {
          return cb(new Error('not found'));
        }
        var p = prev.shift();
        self._xget(p, key, opts, function (err, value, deleted) {
          if (deleted) return cb(err);
          else if (err && notFound(err)) {
            checkp(p, function (err, value) {
              if (err && notFound(err)) next()
              else cb(err, value)
            });
          }
        });
      })();
    });
  }
};

Cow.prototype._iterator = function (opts) {
  return new CowIterator(this, opts);
};

Cow.prototype._put = function (key, value, opts, cb) {
  this._batch([
    { type: 'put', key: key, value: value }
  ], opts, cb);
};

Cow.prototype._del = function (key, opts, cb) {
  this._batch([
    { type: 'del', key: key }
  ], opts, cb);
};

Cow.prototype._batch = function (rows, opts, cb) {
  var self = this;
  var ops = [];
  rows.forEach(function (row) {
    var dkey = self._prefix('d', row.key);
    var nkey = self._prefix('n', row.key);
    if (row.type === 'put') {
      ops.push(
        { type: 'del', key: dkey },
        xtend(row, { key: nkey })
      );
    }
    else if (row.type === 'del') {
      ops.push(
        { type: 'put', key: dkey, value: '0', valueEncoding: 'utf8' },
        xtend(row, { key: nkey })
      );
    }
  });
  self._db.batch(ops, opts, cb);
};

Cow.prototype._getPrev = function (key, cb) {
  var self = this;
  collect(self._db.createReadStream({
    gt: 'l!' + key + '!',
    lt: 'l!' + key + '!~'
  }), onrows);
  
  function onrows (err, rows) {
    if (err) return cb(err);
    var prev = rows.map(function (row) {
      return row.key.split('!')[2];
    });
    cb(null, prev);
  }
};
