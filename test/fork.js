var test = require('tape');
var collect = require('collect-stream');
var forksnap = require('../');
var memdb = require('memdb');

var docs = [
  {
    id: 'A',
    prev: null,
    batch: [
      { type: 'put', key: 'a', value: 100 },
      { type: 'put', key: 'b', value: 200 },
      { type: 'put', key: 'c', value: 300 }
    ]
  },
  {
    id: 'B',
    prev: 'A',
    batch: [
      { type: 'put', key: 'a', value: 123 },
      { type: 'put', key: 'd', value: 400 },
      { type: 'del', key: 'c' },
    ]
  },
  {
    id: 'C',
    prev: 'B',
    batch: [
      { type: 'put', key: 'c', value: 333 }
    ]
  },
  {
    id: 'D',
    prev: 'B',
    batch: [
      { type: 'put', key: 'e', value: 555 }
    ]
  }
];

var expected = {
  A: [
    { key: 'a', value: 100 },
    { key: 'b', value: 200 },
    { key: 'c', value: 300 },
  ],
  B: [
    { key: 'a', value: 123 },
    { key: 'b', value: 200 },
    { key: 'd', value: 400 }
  ],
  C: [
    { key: 'a', value: 123 },
    { key: 'b', value: 200 },
    { key: 'c', value: 333 },
    { key: 'd', value: 400 }
  ],
  D: [
    { key: 'a', value: 123 },
    { key: 'b', value: 200 },
    { key: 'd', value: 400 },
    { key: 'e', value: 555 }
  ]
};

var snap = forksnap(memdb())
test('fork populate', function (t) {
  t.plan(docs.length);
  docs.forEach(function (doc) {
    var c = snap.create(doc.id, doc.prev, { valueEncoding: 'json' });
    c.batch(doc.batch, function (err) {
      t.ifError(err, 'batch ' + doc.id);
    });
  });
});

test('fork', function (t) {
  t.plan(docs.length * 2);
  Object.keys(expected).forEach(function (key) {
    var ex = expected[key];
    var c = snap.open(key, { valueEncoding: 'json' });
    var r = c.createReadStream();
    collect(r, function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, ex, 'sequence ' + key);
    });
  });
});
