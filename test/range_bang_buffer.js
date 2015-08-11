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

test('range bang buffer', function (t) {
  // populate with a linear chain of updates
  var batches = chain.slice();
  t.plan(6);
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
    var c0 = forks.open(0, {
      keyEncoding: 'binary',
      valueEncoding: 'binary'
    });
    collect(c0.createReadStream({ gt: 'x!a' }), function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, [
        { key: Buffer('x!b'), value: Buffer('200') },
        { key: Buffer('x!c'), value: Buffer('300') },
      ], 'seq 0 gt x!a');
    });
  }
});
