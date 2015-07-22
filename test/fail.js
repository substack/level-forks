var test = require('tape');
var collect = require('collect-stream');
var Forks = require('../');
var memdb = require('memdb');
var Down = require('../lib/cowdown.js');

var batches = [
  [
    { type: 'put', key: 'a', value: 100 },
    { type: 'put', key: 'b', value: 200 },
    { type: 'put', key: 'c', value: 300 }
  ],
  [
    { type: 'put', key: 'a', value: 123 },
    { type: 'put', key: 'd', value: 400 },
    { type: 'del', key: 'c' },
  ],
  [
    { type: 'put', key: 'c', value: 333 }
  ],
  [
    { type: 'put', key: 'e', value: 555 }
  ]
];

test('batch fail', function (t) {
  t.plan(1);
  var db = memdb();
  db.batch = function (rows, cb) {
    process.nextTick(function () {
      cb(new Error('whatever'));
    });
  };
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null);
  c0.on('error', function (err) {
    t.equal(err.message, 'whatever');
  });
  c0.batch(batches[0]);
});

test('cowdown get fail', function (t) {
  t.plan(2);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null);
  c0.batch(batches[0], function (err) {
    t.ifError(err);
    
    db.db.get = function (key, opts, cb) {
      cb(new Error('whatever'));
    };
    var d = new Down(db, '0');;
    d.get('a', function (err, value) {
      t.equal(err.message, 'whatever', 'forward error message');
    });
  });
});

test('cowdown iterator fail', function (t) {
  t.plan(2);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null);
  c0.batch(batches[0], function (err) {
    t.ifError(err);
    db.db.iterator = function (opts) {
      return {
        next: function (cb) {
          cb(new Error('xyz'));
        }
      };
    };
    var d = new Down(db, '0');
    var i = d.iterator();
    i.next(function (err, key, value) {
      t.equal(err.message, 'xyz', 'iterator error message');
    });
  });
});
