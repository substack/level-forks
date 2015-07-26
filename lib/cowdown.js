var inherits = require('inherits');
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN;
var prefix = require('./prefix.js');
var xtend = require('xtend');
var notFound = require('./not_found.js');
var CowIterator = require('./iterator.js');
var getPrev = require('./get_prev.js');
var iserr = require('./iserr.js');
var once = require('once');

module.exports = Cow;
inherits(Cow, AbstractLevelDOWN);

function Cow (db, key) {
  if (!(this instanceof Cow)) return new Cow(db, key);
  this._db = db.db || db;
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
  var xkey = prefix('d', pkey, key)
  if (iserr(xkey)) return tick(cb, xkey);
  self._db.get(xkey, opts, function (err, value) {
    if (err && notFound(err)) get()
    else if (err) cb(err)
    else cb(NotFound(), undefined, true)
  });
  function get () {
    var gkey = prefix('n', pkey, key);
    self._db.get(gkey, opts, cb);
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
    getPrev(self._db, pkey, function (err, prev) {
      if (err) return cb(err);
      ;(function next () {
        if (prev.length === 0) {
          return cb(NotFound());
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
          else cb(err, value)
        });
      })();
    });
  }
};

Cow.prototype._iterator = function (opts) {
  return new CowIterator(this._db, this._key, opts);
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
  cb = once(cb);
  var self = this;
  var ops = [];
  rows.forEach(function (row) {
    var dkey = self._prefix('d', row.key);
    if (iserr(dkey)) return cb(dkey);
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
        xtend(row, { type: 'put', key: nkey, value: '0' })
      );
    }
  });
  self._db.batch(ops, opts, cb);
};

function NotFound () {
  var err = new Error('not found');
  err.notFound = true;
  return err;
}

function tick (f) {
  var args = [].slice.call(arguments, 1);
  process.nextTick(function () { f.apply(null, args) });
}
