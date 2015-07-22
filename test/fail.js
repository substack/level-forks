var test = require('tape');
var collect = require('collect-stream');
var Forks = require('../');
var memdb = require('memdb');

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
