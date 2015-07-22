var inherits = require('inherits');
var cowdown = require('level-cowdown');
var defaults = require('level-defaults');
var EventEmitter = require('events').EventEmitter;
var levelup = require('levelup');
var Deferred = require('deferred-leveldown');
var xtend = require('xtend');
var isarray = require('isarray');
var collect = require('collect-stream');
var CowDown = require('./lib/cowdown.js');

module.exports = CF;
inherits(CF, EventEmitter);

function CF (db) {
  if (!(this instanceof CF)) return new CF(db);
  EventEmitter.call(this);
  this.db = defaults(db, { valueEncoding: 'json' });
}

CF.prototype.create = function (key, prev, opts) {
  var self = this;
  if (!opts) opts = {};
  var def = new Deferred('fake');
  
  var ops = (isarray(prev) ? prev : [prev]).filter(Boolean)
    .map(function (p) {
      return { type: 'put', key: 'l!' + key + '!' + p, value: 0 };
    })
  ;
  var updb = up(def, opts);
  self.db.batch(ops, onbatch);
  return updb;
  
  function onbatch (err) {
    if (err) return updb.emit('error', err)
    def.setDb(new CowDown(self.db, key));
  }
};

CF.prototype.open = function (key, opts) {
  return up(new CowDown(this.db, key), opts);
};

function up (down, opts) {
  return levelup('fake', xtend(opts, {
    db: function () { return down }
  }));
}
