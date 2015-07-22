module.exports = function iserr (err) {
  return err && typeof err === 'object' && err instanceof Error;
}
