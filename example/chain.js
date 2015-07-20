var cowchain = require('../');
var memdb = require('memdb');
var chain = cowchain(memdb())

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

// populate the db under a sequence number
(function next (seq) {
  if (batches.length === 0) return ready();
  chain.open(seq).batch(batches.shift(), function (err) {
    if (err) console.error(err)
    else next(seq + 1)
  });
})(0);

function ready () {
  chain.open(0).get('a', function (err, value) {
    console.log('a[0]=', value);
  });
  chain.open(2).get('a', function (err, value) {
    console.log('a[2]=', value);
  });
  chain.open(3).get('c', function (err, value) {
    console.log('c[3]=', value);
  });
}
