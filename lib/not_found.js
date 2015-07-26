module.exports = function notFound (err) {
console.log('ERR=', err);
    return err && (
      err.notFound || err.type === 'NotFoundError'
      || /^NotFound/i.test(err.message)
    );
};
