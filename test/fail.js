var test = require('tape');
var collect = require('collect-stream');
var Forks = require('../');
var memdb = require('memdb');
var memdown = require('memdown');
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

test('cowdown iterator no opts', function (t) {
  t.plan(2);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null);
  c0.batch(batches[0], function (err) {
    t.ifError(err);
    
    var d = new Down(db, '0');;
    d._iterator().next(function (err) {
      t.ifError(err);
    });
  });
});

test('cowdown get prev get fail', function (t) {
  t.plan(3);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null);
  var c1 = forks.create(1, 0);
  c0.batch(batches[0], function (err) {
    t.ifError(err);
    c1.batch(batches[1], function (err) {
      t.ifError(err);
      db.db.iterator = function (opts) {
        return {
          next: function (cb) { cb(new Error('pizza')) }
        };
      };
      var d = new Down(db.db, '1');
      d.get('b', function (err, value) {
        t.equal(err.message, 'pizza');
      });
    });
  });
});

test('cowdown get prev iterator fail', function (t) {
  t.plan(3);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null);
  var c1 = forks.create(1, 0);
  c0.batch(batches[0], function (err) {
    t.ifError(err);
    c1.batch(batches[1], function (err) {
      t.ifError(err);
      db.db.iterator = function (opts) {
        return {
          next: function (cb) { cb(new Error('pizza')) }
        };
      };
      var d = new Down(db.db, '1');
      d.iterator().next(function (err, value) {
        t.equal(err.message, 'pizza');
      });
    });
  });
});

test('missing previous link failure', function (t) {
  t.plan(2);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c1 = forks.create(1, 0);
  c1.batch(batches[1], function (err) {
    t.ifError(err);
    c1.get('x', function (err, value) {
      t.ok(err.notFound);
    });
  });
});

test('iterator get prev fail', function (t) {
  t.plan(3);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null);
  var c1 = forks.create(1, 0);
  c0.batch(batches[0], function (err) {
    t.ifError(err);
    c1.batch(batches[1], function (err) {
      t.ifError(err);
      var iterator = db.db.iterator;
      db.db.iterator = function (opts) {
        if (/^l!0!/.test(opts.gt)) {
          return {
            next: function (cb) {
              cb(new Error('pizza'))
            }
          };
        }
        else return iterator.apply(this, arguments);
      };
      var d = new Down(db.db, '1');
      d.iterator().next(function (err, value) {
        t.equal(err.message, 'pizza');
      });
    });
  });
});

test('unhandled key type', function (t) {
  t.plan(7);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0);
  c0.batch(batches[0], function (err) {
    t.ifError(err);
    var d = new Down(db, '0');
    d.iterator({ gt: [1,2,3] }).next(function (err) {
      t.equal(err.message, 'unhandled key type');
    });
    d.iterator({ gte: [1,2,3] }).next(function (err) {
      t.equal(err.message, 'unhandled key type');
    });
    d.iterator({ lt: [1,2,3] }).next(function (err) {
      t.equal(err.message, 'unhandled key type');
    });
    d.iterator({ lte: [1,2,3] }).next(function (err) {
      t.equal(err.message, 'unhandled key type');
    });
    d._get([1,2,3], {}, function (err) {
      t.equal(err.message, 'unhandled key type');
    });
    d._batch([
      { type: 'put', key: {a:3}, value: '4' }
    ], {}, function (err) {
      t.equal(err.message, 'unhandled key type');
    });
  });
});

test('iterator no opts', function (t) {
  t.plan(3);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0);
  c0.batch(batches[0], function (err) {
    t.ifError(err);
    var d = new Down(db, '0');
    d.iterator().next(function (err, key, value) {
      t.deepEqual(key, new Buffer('a'));
      t.deepEqual(value, new Buffer('100'));
    });
  });
});

test('cursor iterator error', function (t) {
  t.plan(4);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null);
  var c1 = forks.create(1, 0);
  var c2 = forks.create(2, 1);
  var pending = 3;
  c0.batch(batches[0], function (err) {
    t.ifError(err);
    ready();
  });
  c1.batch(batches[1], function (err) {
    t.ifError(err);
    ready();
  });
  c1.batch(batches[2], function (err) {
    t.ifError(err);
    ready();
  });
  function ready () {
    if (--pending !== 0) return;
    var iterator = db.db.iterator;
    var n = 0;
    db.db.iterator = function (opts) {
      if (++n === 3) {
        return {
          next: function (cb) { cb(new Error('pizza')) }
        };
      }
      else return iterator.apply(this, arguments);
    };
    var d = new Down(db.db, '2');
    var it = d.iterator();
    it.next(function (err, k0, v0) {
      t.equal(err.message, 'pizza', 'cursor error');
    });
  }
});

test('iterator get deleted fail', function (t) {
  t.plan(batches.length + 2);
  var db = memdown();
  var get = db.get;
  db.get = function (key, cb) {
    cb(new Error('hey what'));
  };
  var forks = Forks(db, { valueEncoding: 'json' });
  (function next (seq, prev) {
    if (!batches[seq]) return ready();
    var c = forks.create(seq, prev);
    c.batch(batches[seq], function (err) {
      t.ifError(err);
      next(seq+1, seq);
    });
  })(0, null);
  
  function ready (err) {
    t.ifError(err);
    var c2 = forks.open(2);
    var r = c2.createReadStream();
    collect(r, function (err, rows) {
      t.equal(err.message, 'hey what');
    });
  }
});

test('create fail', function (t) {
  t.plan(1);
  var db = memdb();
  db.batch = function (rows, cb) {
    process.nextTick(function () {
      cb(new Error('whatever'));
    });
  };
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null, function (err) {
    t.equal(err.message, 'whatever');
  });
  c0.batch(batches[0]);
});

test('prebatch fail', function (t) {
  t.plan(1);
  var db = memdb();
  var forks = Forks(db, { valueEncoding: 'json' });
  var c0 = forks.create(0, null, { prebatch: prebatch });
  c0.batch(batches[0], function (err) {
    t.equal(err.message, 'whatever');
  });
  function prebatch (ops, cb) { cb(new Error('whatever')) }
});
