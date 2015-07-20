var cowfork = require('../');
var memdb = require('memdb');
var cow = cowfork(memdb())

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

// populate with a linear chain of updates
(function next (seq, prev) {
  if (batches.length === 0) return ready();
  cow.create(seq, prev).batch(batches.shift(), function (err) {
    if (err) console.error(err)
    else next(seq + 1, seq)
  });
})(0, null);

function ready () {
  cow.open(0).get('a', function (err, value) {
    console.log('a[0]=', value);
  });
  cow.open(2).get('a', function (err, value) {
    console.log('a[2]=', value);
  });
  cow.open(3).get('c', function (err, value) {
    console.log('c[3]=', value);
  });
}
