var xtend = require('xtend');
var iserr = require('./iserr.js');
var prefix = require('./prefix.js');

module.exports = function fix (key, opts) {
  if (!opts) opts = {};
  opts = xtend(opts);
  if (opts.gt) {
    opts.gt = prefix('n', key, opts.gt);
    if (iserr(opts.gt)) return opts.gt;
  }
  if (opts.gte) {
    opts.gte = prefix('n', key, opts.gte);
    if (iserr(opts.gte)) return opts.gte;
  }
  if (opts.lt) {
    opts.lt = prefix('n', key, opts.lt);
    if (iserr(opts.lt)) return opts.lt;
  }
  if (opts.lte) {
    opts.lte = prefix('n', key, opts.lte);
    if (iserr(opts.lte)) return opts.lte;
  }
  if (!opts.gt && !opts.gte) {
    opts.gt = 'n!' + key + '!';
    if (iserr(opts.gt)) return opts.gt;
  }
  if (!opts.lt && !opts.lte) {
    opts.lt = 'n!' + key + '!\uffff';
    if (iserr(opts.lt)) return opts.lt;
  }
  return opts;
}
