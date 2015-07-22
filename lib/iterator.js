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
  this._rows = [];
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
    if (self._rows[i] === undefined) {
      pending ++;
      self._cursors[i].next((function (i) {
        return function (err, key, value) {
          if (err) return cb(err);
          if (key === undefined) {
            self._cursors[i] = null;
            if (-- pending === 0) done();
            return;
          }
          self._rows[i] = { key: key.split('!')[2], value: value };
          
          self._db.get('d!' + key.slice(2), function (err, value) {
            if (err && notFound(err)) {}
            else if (err) return cb(err);
            else self._rows[i].removed = true;
            if (-- pending === 0) done();
          });
        };
      })(i));
    }
  }
  
  function done () {
    var mink, minv, mini, minr;
    for (var i = 0; i < self._rows.length; i++) {
      if (!self._rows[i]) continue;
      mink = self._rows[i].key;
      mini = i;
      minv = self._rows[i].value;
      minr = self._rows[i].removed;
      break;
    }
    var skip = 0;
    
    for (var i = 1; i < self._rows.length; i++) {
      if (!self._cursors[i]) { skip++; continue }
      var key = self._rows[i].key;
      if (key === mink) {
        self._rows[i] = undefined;
      }
      else if (key < mink) {
        mink = key;
        mini = i;
        minv = self._rows[i].value;
        minr = self._rows[i].removed;
      }
    }
    if (skip === self._rows.length) {
      return cb(null, undefined, undefined);
    }
    self._rows[mini] = undefined;
    if (minr) self._next(cb);
    else cb(null, mink, minv);
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
