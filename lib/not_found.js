module.exports = function notFound (err) {
    return err && (
      err.notFound || err.type === 'NotFoundError'
      || err.message === 'NotFound'
    );
};
