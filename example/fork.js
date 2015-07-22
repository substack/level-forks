var collect = require('collect-stream');
var cowfork = require('../');
var memdb = require('memdb');
var cow = cowfork(memdb())

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
  var c = cow.create(doc.id, doc.prev, { valueEncoding: 'json' });
  c.batch(doc.batch, function (err) {
    if (err) console.error(err);
    if (--pending === 0) ready();
  });
});

function ready () {
  cow.open('C').createReadStream().on('data', console.log);
}
