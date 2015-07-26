# level-forks

create a forking graph of cascading leveldb namespaces

# example

``` js
var collect = require('collect-stream');
var Forks = require('level-forks');
var level = require('level');
var forks = Forks(level('./db'));

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
  var c = forks.create(doc.id, doc.prev, { valueEncoding: 'json' });
  c.batch(doc.batch, function (err) {
    if (err) console.error(err);
    if (--pending === 0) ready();
  });
});

function ready () {
  forks.open('C').createReadStream().on('data', console.log);
}
```

output:

```
{ key: 'a', value: '123' }
{ key: 'b', value: '200' }
{ key: 'c', value: '333' }
{ key: 'd', value: '400' }
```

# api

``` js
var Forks = require('level-forks')
```

## var forks = Forks(db, opts)

Create a new instance `forks` from a levelup or leveldown handle `db`.

Options from `opts` are passed to levelup so you can set things like
`opts.valueEncoding` and `opts.keyEncoding`.

## var db = forks.create(key, prev=[], opts={}, cb)

Create a new `db` identified by `key` that link back to the keys in `prev`,
if any.

`db` is a levelup handle that represents the database at `key` in the graph.

When the links are written, `cb(err, db)` fires with any errors `err` or the
database handle `db`.

Set encodings with `opts.valueEncoding` or `opts.keyEncoding` or:

* `opts.prebatch(function (rows, cb) {})` - intercept batches, calling
`cb(err, rows)` with rows to insert on a batch

## var db = forks.open(key, opts)

Open an existing levelup handle by its `key`.

Set encodings with `opts.valueEncoding` or `opts.keyEncoding` or:

* `opts.prebatch(function (rows, cb) {})` - intercept batches, calling
`cb(err, rows)` with rows to insert on a batch

# install

```
npm install level-forks
```

# license

MIT
