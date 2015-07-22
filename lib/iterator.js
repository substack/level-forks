var inherits = require('inherits');
var AbstractIterator = require('abstract-leveldown').AbstractIterator;
var getPrev = require('./get_prev.js');
var prefix = require('./prefix.js');
var notFound = require('./not_found.js');
var once = require('once');
var concat = require('concat-map');
var EventEmitter = require('events').EventEmitter;
var xtend = require('xtend');

module.exports = Iterator;
inherits(Iterator, AbstractIterator);

function Iterator (db, key, opts) {
  var self = this;
  AbstractIterator.call(this);
  this._db = db;
  this._key = key;
  this._cursors = [];
  this._cursors[0] = [db.iterator(fix(this._key, opts))];
  this._keys = [];
  this._values = [];
  this._removed = [];
  this._ready = false;
  this._ev = new EventEmitter;
  
  getPrev(self._db, self._key, fn(1, function (err) {
    self._ready = true;
    self._cursors = concat(self._cursors, identity);
    self._ev.emit('_ready');
  }));
  
  function fn (n, cb) {
    cb = once(cb);
    if (!self._cursors[n]) self._cursors[n] = [];
    var pending = 0;
    return function (err, prev) {
      if (err) return cb(err);
      prev.forEach(function (p) {
        self._cursors[n].push(self._db.iterator(fix(p, opts)));
        pending ++;
        getPrev(self._db, p, fn(n+1, function (err) {
          if (err) cb(err)
          else if (-- pending === 0) cb(null)
        }));
      });
      if (pending === 0) cb(null);
    };
  }
}

Iterator.prototype._next = function (cb) {
  var self = this;
  if (!self._ready) {
    return self._ev.once('_ready', function () { self._next(cb) });
  }
  var pending = 0;
  for (var i = 0; i < self._cursors.length; i++) {
    if (!self._cursors[i]) continue;
    if (self._keys[i] === undefined) {
      pending ++;
      self._cursors[i].next((function (i) {
        return function (err, key, value) {
          if (err) return cb(err);
          if (key === undefined) {
            self._cursors[i] = null;
          }
          else {
            self._keys[i] = key;
            self._values[i] = value;
          }
          if (-- pending === 0) done();
        };
      })(i));
    }
  }
  
  function done () {
    var mink, minv, mini;
    for (var i = 0; i < self._keys.length; i++) {
      if (!self._keys[i]) continue;
      mink = self._keys[i].split('!')[2];
      mini = i;
      minv = self._values[i];
      break;
    }
    var skip = 0;
    
    for (var i = 1; i < self._keys.length; i++) {
      if (!self._cursors[i]) { skip++; continue }
      var key = self._keys[i].split('!')[2];
      if (key === mink) {
        self._keys[i] = undefined;
        self._values[i] = undefined;
      }
      else if (key < mink) {
        mink = key;
        mini = i;
        minv = self._values[i];
      }
    }
    if (skip === self._keys.length) {
      return cb(null, undefined, undefined);
    }
    self._keys[mini] = undefined;
    self._values[mini] = undefined;
    cb(null, mink, minv);
  }
};

function fix (key, opts) {
  if (!opts) opts = {};
  opts = xtend(opts);
  if (opts.gt) opts.gt = prefix('n', key, opts.gt);
  if (opts.gte) opts.gte = prefix('n', key, opts.gte);
  if (opts.lt) opts.lt = prefix('n', key, opts.lt);
  if (opts.lte) opts.lte = prefix('n', key, opts.lte);
  if (!opts.gt && !opts.gte) opts.gt = 'n!' + key + '!';
  if (!opts.lt && !opts.lte) opts.lt = 'n!' + key + '!\uffff';
  return opts;
}

function identity (x) { return x }
