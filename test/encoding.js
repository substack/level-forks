var test = require('tape');
var collect = require('collect-stream');
var forksnap = require('../');
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

test('encoding', function (t) {
  // populate with a linear chain of updates
  var batches = chain.slice();
  t.plan(batches.length + 4);
  var snap = forksnap(memdb(), { valueEncoding: 'json' });
  
  ;(function next (seq, prev) {
    if (batches.length === 0) return ready();
    var c = snap.create(seq, prev);
    c.batch(batches.shift(), function (err) {
      t.ifError(err, 'batch ' + seq);
      next(seq + 1, seq)
    });
  })(0, null);
  
  function ready () {
    var c0 = snap.open(0);
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
  }
});
