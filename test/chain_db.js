var test = require('tape');
var collect = require('collect-stream');
var Forks = require('../');
var level = require('level-test')();

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

var gets = [
  [
    { key: 'a', value: 100 },
    { key: 'b', value: 200 },
    { key: 'c', value: 300 },
    { key: 'd' },
    { key: 'e' },
    { key: 'f' }
  ],
  [
    { key: 'a', value: 123 },
    { key: 'b', value: 200 },
    { key: 'c' },
    { key: 'd', value: 400 },
    { key: 'e' },
    { key: 'f' }
  ],
  [
    { key: 'a', value: 123 },
    { key: 'b', value: 200 },
    { key: 'c', value: 333 },
    { key: 'd', value: 400 },
    { key: 'e' },
    { key: 'f' }
  ],
  [
    { key: 'a', value: 123 },
    { key: 'b', value: 200 },
    { key: 'c', value: 333 },
    { key: 'd', value: 400 },
    { key: 'e', value: 555 },
    { key: 'f' }
  ]
];

test('chain level-test', function (t) {
  t.plan(51);
  
  var forks = Forks(level('ch-' + Math.random()))
  // populate with a linear chain of updates
  var batches = chain.slice();
  
  ;(function next (seq, prev) {
    if (batches.length === 0) return ready();
    var c = forks.create(seq, prev, { valueEncoding: 'json' });
    c.batch(batches.shift(), function (err) {
      t.ifError(err, 'batch ' + seq);
      next(seq + 1, seq)
    });
  })(0, null);
  
  function ready () {
    expected.forEach(function (ex, seq) {
      var c = forks.open(seq, { valueEncoding: 'json' });
      var r = c.createReadStream();
      collect(r, function (err, rows) {
        t.ifError(err);
        t.deepEqual(rows, ex, 'sequence ' + seq);
      });
      gets[seq].forEach(function (row) {
        c.get(row.key, function (err, value) {
          if ('value' in row) {
            t.ifError(err);
            t.deepEqual(value, row.value, '.get("' + row.key + '") value');
          }
          else {
            t.equal(err.type, 'NotFoundError', row.key + ' not set');
          }
        });
      });
    });
  }
});
