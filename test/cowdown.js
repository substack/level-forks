var test = require('tape');
var Forks = require('../');
var memdb = require('memdb');
var Down = require('../lib/cowdown.js');

test('cowdown', function (t) {
  t.plan(3);
  var c = Down(memdb());
  c.put('wow', '123', function (err) {
    t.ifError(err);
    c.get('wow', function (err, value) {
      t.ifError(err);
      t.deepEqual(value, Buffer('123'));
    });
  });
});

test('new cowdown', function (t) {
  t.plan(3);
  var c = new Down(memdb());
  c.put('wow', '123', function (err) {
    t.ifError(err);
    c.get('wow', function (err, value) {
      t.ifError(err);
      t.deepEqual(value, Buffer('123'));
    });
  });
});
