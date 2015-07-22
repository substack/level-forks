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

var expected = [
  [
    { key: 'a', value: 100 },
    { key: 'b', value: 200 },
    { key: 'c', value: 300 },
  ],
  [
    { key: 'a', value: 123 },
    { key: 'b', value: 200 },
    { key: 'd', value: 400 }
  ],
  [
    { key: 'a', value: 123 },
    { key: 'b', value: 200 },
    { key: 'c', value: 333 },
    { key: 'd', value: 400 }
  ],
  [
    { key: 'a', value: 123 },
    { key: 'b', value: 200 },
    { key: 'c', value: 333 },
    { key: 'd', value: 400 },
    { key: 'e', value: 555 }
  ]
];

test('range', function (t) {
  // populate with a linear chain of updates
  var batches = chain.slice();
  t.plan(batches.length + 14*4);
  var cow = cowfork(memdb())
  
  ;(function next (seq, prev) {
    if (batches.length === 0) return ready();
    var c = cow.create(seq, prev, { valueEncoding: 'json' });
    c.batch(batches.shift(), function (err) {
      t.ifError(err, 'batch ' + seq);
      next(seq + 1, seq)
    });
  })(0, null);
  
  function ready () {
    var c0 = cow.open(0, { valueEncoding: 'json' });
    collect(c0.createReadStream({ gt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'b', value: 200 },
        { key: 'c', value: 300 },
      ], 'seq 0 gt a');
    });
    collect(c0.createReadStream({ lt: 'd' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 100 },
        { key: 'b', value: 200 },
        { key: 'c', value: 300 },
      ], 'seq 0 lt d');
    });
    collect(c0.createReadStream({ lt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [], 'seq 0 lt a');
    });
    collect(c0.createReadStream({ lte: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 100 }
      ], 'seq 0 lte a');
    });
    collect(c0.createReadStream({ gt: 'b' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'c', value: 300 }
      ], 'seq 0 gt b');
    });
    collect(c0.createReadStream({ lt: 'c', gt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'b', value: 200 }
      ], 'seq 0 lt c gt a');
    });
    collect(c0.createReadStream({ lt: 'c', gte: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 100 },
        { key: 'b', value: 200 }
      ], 'seq 0 lt c gte a');
    });
    
    var c1 = cow.open(1, { valueEncoding: 'json' });
    collect(c1.createReadStream({ gt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'b', value: 200 },
        { key: 'd', value: 400 },
      ], 'seq 1 gt a');
    });
    collect(c1.createReadStream({ lt: 'd' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 123 },
        { key: 'b', value: 200 }
      ], 'seq 1 lt d');
    });
    collect(c1.createReadStream({ lt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [], 'seq 1 lt a');
    });
    collect(c1.createReadStream({ lte: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 123 }
      ], 'seq 1 lte a');
    });
    collect(c1.createReadStream({ gt: 'b' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'd', value: 400 }
      ], 'seq 1 gt b');
    });
    collect(c1.createReadStream({ lt: 'c', gt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'b', value: 200 }
      ], 'seq 1 lt c gt a');
    });
    collect(c1.createReadStream({ lt: 'c', gte: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 123 },
        { key: 'b', value: 200 }
      ], 'seq 1 lt c gte a');
    });
    
    var c2 = cow.open(2, { valueEncoding: 'json' });
    collect(c2.createReadStream({ gt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'b', value: 200 },
        { key: 'c', value: 333 },
        { key: 'd', value: 400 }
      ], 'seq 2 gt a');
    });
    collect(c2.createReadStream({ lt: 'd' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 123 },
        { key: 'b', value: 200 },
        { key: 'c', value: 333 }
      ], 'seq 2 lt d');
    });
    collect(c2.createReadStream({ lt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [], 'seq 2 lt a');
    });
    collect(c2.createReadStream({ lte: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 123 }
      ], 'seq 2 lte a');
    });
    collect(c2.createReadStream({ gt: 'b' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'c', value: 333 },
        { key: 'd', value: 400 }
      ], 'seq 2 gt b');
    });
    collect(c2.createReadStream({ lt: 'c', gt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'b', value: 200 }
      ], 'seq 2 lt c gt a');
    });
    collect(c2.createReadStream({ lt: 'c', gte: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 123 },
        { key: 'b', value: 200 }
      ], 'seq 2 lt c gte a');
    });
    
    var c3 = cow.open(3, { valueEncoding: 'json' });
    collect(c3.createReadStream({ gt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'b', value: 200 },
        { key: 'c', value: 333 },
        { key: 'd', value: 400 },
        { key: 'e', value: 555 }
      ], 'seq 3 gt a');
    });
    collect(c3.createReadStream({ lt: 'd' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 123 },
        { key: 'b', value: 200 },
        { key: 'c', value: 333 }
      ], 'seq 3 lt d');
    });
    collect(c3.createReadStream({ lt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [], 'seq 3 lt a');
    });
    collect(c3.createReadStream({ lte: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 123 }
      ], 'seq 3 lte a');
    });
    collect(c3.createReadStream({ gt: 'b' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'c', value: 333 },
        { key: 'd', value: 400 },
        { key: 'e', value: 555 }
      ], 'seq 3 gt b');
    });
    collect(c3.createReadStream({ lt: 'c', gt: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'b', value: 200 }
      ], 'seq 3 lt c gt a');
    });
    collect(c3.createReadStream({ lt: 'c', gte: 'a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: 'a', value: 123 },
        { key: 'b', value: 200 }
      ], 'seq 3 lt c gte a');
    });
  }
});
