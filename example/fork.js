var collect = require('collect-stream');
var forksnap = require('../');
var memdb = require('memdb');
var snap = forksnap(memdb());

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

var pending = docs.length;
docs.forEach(function (doc) {
  var c = snap.create(doc.id, doc.prev, { valueEncoding: 'json' });
  c.batch(doc.batch, function (err) {
    if (err) console.error(err);
    if (--pending === 0) ready();
  });
});

function ready () {
  snap.open('C').createReadStream().on('data', console.log);
}
