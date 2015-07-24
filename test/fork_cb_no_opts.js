var test = require('tape');
var collect = require('collect-stream');
var Forks = require('../');
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

var forks = Forks(memdb())
test('forkcb no opts populate', function (t) {
  t.plan(docs.length * 2);
  docs.forEach(function (doc) {
    var c = forks.create(
      doc.id,
      doc.prev,
      oncreate
    );
    function oncreate (err) {
      t.ifError(err);
      c.batch(doc.batch, { valueEncoding: 'json' }, function (err) {
        t.ifError(err, 'batch ' + doc.id);
      });
    }
  });
});

test('forkcb no opts', function (t) {
  t.plan(docs.length * 2);
  Object.keys(expected).forEach(function (key) {
    var ex = expected[key];
    var c = forks.open(key, { valueEncoding: 'json' });
    var r = c.createReadStream();
    collect(r, function (err, rows) {
      t.ifError(err);
      t.deepEqual(rows, ex, 'sequence ' + key);
    });
  });
});
