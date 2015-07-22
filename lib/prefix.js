module.exports = function prefix (pre, seq, key) {
    if (typeof key === 'string') return pre + '!' + seq + '!' + key;
    if (Buffer.isBuffer(key)) {
        return Buffer.concat([ new Buffer(pre + '!' + seq + '!'), key ]);
    }
    throw new Error('unhandled key type');
};
