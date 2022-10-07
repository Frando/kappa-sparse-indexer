const debugLock = require('debug')('indexer:lock')
const { promisify } = require('util')
const streamCollector = require('stream-collector')

exports.transformPipeline = function (state, fns, final) {
  let idx = -1
  next(state)
  function next (state) {
    idx = idx + 1
    if (state === null) return final(state)
    if (idx === fns.length) return final(state)
    if (!fns[idx]) return next(state)
    fns[idx](state, next)
  }
}

exports.maybeCallback = function (callback) {
  if (typeof callback === 'function' && callback.promise) {
    callback.promise = undefined
  }
  if (callback) {
    return callback
  }
  let _resolve, _reject
  callback = function (err, result) {
    if (err) _reject(err)
    else _resolve(result)
  }
  callback.promise = new Promise((resolve, reject) => {
    _resolve = resolve
    _reject = reject
  })
  return callback
}

exports.loggingMutex = function (lock, ns) {
  return (cb, name) => {
    debugLock('want', ns, name)
    lock((release, ...args) => {
      debugLock('hold', ns, name)
      const release2 = (...args) => {
        debugLock('free', ns, name)
        release(...args)
      }
      cb(release2, ...args)
    })
  }
}

exports.collectStream = promisify(streamCollector)
