module.exports = function notFound (err) {
    return err && (
      err.notFound || err.type === 'NotFoundError'
      || /^NotFound/i.test(err.message)
    );
};
