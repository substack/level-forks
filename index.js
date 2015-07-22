var inherits = require('inherits');
var EventEmitter = require('events').EventEmitter;
var levelup = require('levelup');
var Deferred = require('deferred-leveldown');
var xtend = require('xtend');
var isarray = require('isarray');
var CowDown = require('./lib/cowdown.js');

module.exports = CF;
inherits(CF, EventEmitter);

function CF (db, opts) {
  if (!(this instanceof CF)) return new CF(db, opts);
  EventEmitter.call(this);
  this._options = opts;
  this.db = db;
}

CF.prototype.create = function (key, prev, opts) {
  var self = this;
  if (!opts) opts = {};
  var def = new Deferred('fake');
  
  var ops = (isarray(prev) ? prev : [prev]).filter(notNullOrUndef)
    .map(function (p) {
      return {
        type: 'put',
        key: 'l!' + key + '!' + p,
        value: '0',
        keyEncoding: 'utf8',
        valueEncoding: 'utf8'
      };
    })
  ;
  var updb = up(def, xtend(self._options, opts));
  self.db.batch(ops, onbatch);
  return updb;
  
  function onbatch (err) {
    if (err) updb.emit('error', err);
    def.setDb(new CowDown(self.db, key));
  }
};

CF.prototype.open = function (key, opts) {
  return up(new CowDown(this.db, key), xtend(this._options, opts));
};

function up (down, opts) {
  return levelup('fake', xtend(opts, {
    db: function () { return down }
  }));
}

function notNullOrUndef (x) { return x !== undefined && x !== null }
