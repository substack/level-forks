var test = require('tape');
var collect = require('collect-stream');
var Forks = require('../');
var memdb = require('memdb');

var chain = [
  [
    { type: 'put', key: 'x!a', value: 100 },
    { type: 'put', key: 'x!b', value: 200 },
    { type: 'put', key: 'x!c', value: 300 }
  ],
  [
    { type: 'put', key: 'x!a', value: 123 },
    { type: 'put', key: 'x!d', value: 400 },
    { type: 'del', key: 'x!c' },
  ],
  [
    { type: 'put', key: 'x!c', value: 333 }
  ],
  [
    { type: 'put', key: 'x!e', value: 555 }
  ]
];

test('range bang', function (t) {
  // populate with a linear chain of updates
  var batches = chain.slice();
  t.plan(batches.length + 14*4);
  var forks = Forks(memdb())
  
  ;(function next (seq, prev) {
    if (batches.length === 0) return ready();
    var c = forks.create(seq, prev, { valueEncoding: 'json' });
    c.batch(batches.shift(), function (err) {
      t.ifError(err, 'batch ' + seq);
      next(seq + 1, seq)
    });
  })(0, null);
  
  function ready () {
    var c0 = forks.open(0, { valueEncoding: 'json' });
    collect(c0.createReadStream({ gt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!b', value: 200 },
        { key: 'x!c', value: 300 },
      ], 'seq 0 gt x!a');
    });
    collect(c0.createReadStream({ lt: 'x!d' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 100 },
        { key: 'x!b', value: 200 },
        { key: 'x!c', value: 300 },
      ], 'seq 0 lt x!d');
    });
    collect(c0.createReadStream({ lt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [], 'seq 0 lt x!a');
    });
    collect(c0.createReadStream({ lte: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 100 }
      ], 'seq 0 lte x!a');
    });
    collect(c0.createReadStream({ gt: 'x!b' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!c', value: 300 }
      ], 'seq 0 gt x!b');
    });
    collect(c0.createReadStream({ lt: 'x!c', gt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!b', value: 200 }
      ], 'seq 0 lt x!c gt x!a');
    });
    collect(c0.createReadStream({ lt: 'x!c', gte: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 100 },
        { key: 'x!b', value: 200 }
      ], 'seq 0 lt x!c gte x!a');
    });
    
    var c1 = forks.open(1, { valueEncoding: 'json' });
    collect(c1.createReadStream({ gt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!b', value: 200 },
        { key: 'x!d', value: 400 },
      ], 'seq 1 gt x!a');
    });
    collect(c1.createReadStream({ lt: 'x!d' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 123 },
        { key: 'x!b', value: 200 }
      ], 'seq 1 lt x!d');
    });
    collect(c1.createReadStream({ lt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [], 'seq 1 lt x!a');
    });
    collect(c1.createReadStream({ lte: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 123 }
      ], 'seq 1 lte x!a');
    });
    collect(c1.createReadStream({ gt: 'x!b' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!d', value: 400 }
      ], 'seq 1 gt x!b');
    });
    collect(c1.createReadStream({ lt: 'x!c', gt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!b', value: 200 }
      ], 'seq 1 lt x!c gt x!a');
    });
    collect(c1.createReadStream({ lt: 'x!c', gte: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 123 },
        { key: 'x!b', value: 200 }
      ], 'seq 1 lt x!c gte x!a');
    });
    
    var c2 = forks.open(2, { valueEncoding: 'json' });
    collect(c2.createReadStream({ gt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!b', value: 200 },
        { key: 'x!c', value: 333 },
        { key: 'x!d', value: 400 }
      ], 'seq 2 gt x!a');
    });
    collect(c2.createReadStream({ lt: 'x!d' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 123 },
        { key: 'x!b', value: 200 },
        { key: 'x!c', value: 333 }
      ], 'seq 2 lt x!d');
    });
    collect(c2.createReadStream({ lt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [], 'seq 2 lt x!a');
    });
    collect(c2.createReadStream({ lte: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 123 }
      ], 'seq 2 lte x!a');
    });
    collect(c2.createReadStream({ gt: 'x!b' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!c', value: 333 },
        { key: 'x!d', value: 400 }
      ], 'seq 2 gt x!b');
    });
    collect(c2.createReadStream({ lt: 'x!c', gt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!b', value: 200 }
      ], 'seq 2 lt x!c gt x!a');
    });
    collect(c2.createReadStream({ lt: 'x!c', gte: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 123 },
        { key: 'x!b', value: 200 }
      ], 'seq 2 lt x!c gte x!a');
    });
    
    var c3 = forks.open(3, { valueEncoding: 'json' });
    collect(c3.createReadStream({ gt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!b', value: 200 },
        { key: 'x!c', value: 333 },
        { key: 'x!d', value: 400 },
        { key: 'x!e', value: 555 }
      ], 'seq 3 gt x!a');
    });
    collect(c3.createReadStream({ lt: 'x!d' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 123 },
        { key: 'x!b', value: 200 },
        { key: 'x!c', value: 333 }
      ], 'seq 3 lt x!d');
    });
    collect(c3.createReadStream({ lt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [], 'seq 3 lt x!a');
    });
    collect(c3.createReadStream({ lte: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 123 }
      ], 'seq 3 lte x!a');
    });
    collect(c3.createReadStream({ gt: 'x!b' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!c', value: 333 },
        { key: 'x!d', value: 400 },
        { key: 'x!e', value: 555 }
      ], 'seq 3 gt x!b');
    });
    collect(c3.createReadStream({ lt: 'x!c', gt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!b', value: 200 }
      ], 'seq 3 lt x!c gt x!a');
    });
    collect(c3.createReadStream({ lt: 'x!c', gte: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'x!a', value: 123 },
        { key: 'x!b', value: 200 }
      ], 'seq 3 lt x!c gte x!a');
    });
  }
});
