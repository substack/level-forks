var test = require('tape');
var collect = require('collect-stream');
var cowfork = require('../');
var memdb = require('memdb');

var chain = [
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

var cow = cowfork(memdb(), { valueEncoding: 'json' });

test('modify', function (t) {
  // populate with a linear chain of updates
  var batches = chain.slice();
  t.plan(batches.length + 16);
  
  ;(function next (seq, prev) {
    if (batches.length === 0) return modify();
    var c = cow.create(seq, prev);
    c.batch(batches.shift(), function (err) {
      t.ifError(err, 'batch ' + seq);
      next(seq + 1, seq)
    });
  })(0, null);
  
  function modify () {
    var c0 = cow.open(0);
    c0.get('b', function (err, value) {
      t.ifError(err);
      t.equal(value, 200);
      c0.del('b', function (err) {
        t.ifError(err);
        cow.open(0).get('b', function (err) {
          t.ok(err.notFound);
        });
        cow.open(2).get('b', function (err) {
          t.ok(err.notFound);
        });
      });
    });
    
    c0.del('c', function (err, value) {
      t.ifError(err);
      cow.open(1).get('c', function (err) {
        t.ok(err.notFound);
      });
      cow.open(2).get('c', function (err, value) {
        t.ifError(err);
        t.equal(value, 333);
      });
    });
    
    var c1 = cow.open(1);
    c1.put('x', 5000, function (err) {
      t.ifError(err);
      cow.open(3).del('x', function (err) {
        t.ifError(err);
        cow.open(2).get('x', function (err, value) {
          t.ifError(err);
          t.equal(value, 5000);
        });
        cow.open(0).get('x', function (err) {
          t.ok(err.notFound);
        });
        cow.open(1).get('x', function (err, value) {
          t.ifError(err);
          t.equal(value, 5000);
        });
      });
    });
  }
});
 
test('modify range check', function (t) {
  t.plan(8);
  collect(cow.open(0).createReadStream(), function (err, rows) {
    t.ifError(err);
    t.deepEqual(rows, [
      { key: 'a', value: 100 }
    ], 'seq 0');
  });
  collect(cow.open(1).createReadStream(), function (err, rows) {
    t.ifError(err);
    t.deepEqual(rows, [
      { key: 'a', value: 123 },
      { key: 'd', value: 400 },
      { key: 'x', value: 5000 }
    ], 'seq 1');
  });
  collect(cow.open(2).createReadStream(), function (err, rows) {
    t.ifError(err);
    t.deepEqual(rows, [
      { key: 'a', value: 123 },
      { key: 'c', value: 333 },
      { key: 'd', value: 400 },
      { key: 'x', value: 5000 }
    ], 'seq 2');
  });
  collect(cow.open(3).createReadStream(), function (err, rows) {
    t.ifError(err);
    t.deepEqual(rows, [
      { key: 'a', value: 123 },
      { key: 'c', value: 333 },
      { key: 'd', value: 400 },
      { key: 'e', value: 555 }
    ], 'seq 3');
  });
});
