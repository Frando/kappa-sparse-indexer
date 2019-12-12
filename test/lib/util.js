exports.runAll = function runAll (ops) {
  return new Promise((resolve, reject) => {
    runNext(ops.shift())
    function runNext (op) {
      op(err => {
        if (err) return reject(err)
        let next = ops.shift()
        if (!next) return resolve()
        return runNext(next)
      })
    }
  })
}

exports.replicate = function replicate (a, b, opts, cb) {
  if (typeof opts === 'function') return replicate(a, b, null, opts)
  if (!opts) opts = { live: true }
  const stream = a.replicate(true, opts)
  stream.pipe(b.replicate(false, opts)).pipe(stream)
  if (cb) setImmediate(cb)
}

// exports.ready = function ready (objs, cb) {
//   next()
//   function next (err) {
//     console.log('NEXT', objs.length, err)
//     if (err) return cb(err)
//     const obj = objs.shift()
//     console.log('ret', typeof obj)
//     if (!obj) return cb()
//     obj.ready(next)
//   }
// }
