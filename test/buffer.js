var test = require('tape');
var collect = require('collect-stream');
var Forks = require('../');
var memdb = require('memdb');

var chain = [
  [
    { type: 'put', key: Buffer('a'), value: 100 },
    { type: 'put', key: Buffer('b'), value: 200 },
    { type: 'put', key: Buffer('c'), value: 300 }
  ],
  [
    { type: 'put', key: Buffer('a'), value: 123 },
    { type: 'put', key: Buffer('d'), value: 400 },
    { type: 'del', key: Buffer('c') },
  ],
  [
    { type: 'put', key: Buffer('c'), value: 333 }
  ],
  [
    { type: 'put', key: Buffer('e'), value: 555 }
  ]
];

test('buffer', function (t) {
  // populate with a linear chain of updates
  var batches = chain.slice();
  t.plan(batches.length + 4);
  var forks = Forks(memdb(), { keyEncoding: 'buffer', valueEncoding: 'json' });
  
  ;(function next (seq, prev) {
    if (batches.length === 0) return ready();
    var c = forks.create(seq, prev);
    c.batch(batches.shift(), function (err) {
      t.ifError(err, 'batch ' + seq);
      next(seq + 1, seq)
    });
  })(0, null);
  
  function ready () {
    var c0 = forks.open(0);
    collect(c0.createReadStream({ gt: Buffer('a') }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: Buffer('b'), value: 200 },
        { key: Buffer('c'), value: 300 },
      ], 'seq 0 gt a');
    });
    collect(c0.createReadStream({ lt: Buffer('d') }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: Buffer('a'), value: 100 },
        { key: Buffer('b'), value: 200 },
        { key: Buffer('c'), value: 300 },
      ], 'seq 0 lt d');
    });
  }
});
