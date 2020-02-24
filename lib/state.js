module.exports = class SimpleState {
  constructor (db, prefix) {
    this.db = db || new MemDB()
    this._prefix = prefix || '_'
    this._STATE = this._prefix + '!s!'
    this._VERSION = this._prefix + '!v!'
    // Bind public methods so that they can be passed on directly.
    this.prefix = this.prefix.bind(this)
    this.reset = this.reset.bind(this)
    this.get = this.get.bind(this)
    this.put = this.put.bind(this)
    this.storeVersion = this.storeVersion.bind(this)
    this.fetchVersion = this.fetchVersion.bind(this)
  }

  prefix (prefix) {
    return new SimpleState(this.db, this._prefix + '/' + prefix)
  }

  reset (cb) {
    const ops = [this._STATE, this._VERSION].map(key => ({ key, type: 'del' }))
    this.db.batch(ops, cb)
  }

  get (cb) {
    getInt(this.db, this._STATE, -1, cb)
  }

  put (seq, cb = noop) {
    const value = String(seq)
    this.db.put(this._STATE, value, cb)
  }

  putVersion (version, cb = noop) {
    this.fetchVersion((err, oldVersion) => {
      if (err) return cb(err)
      if (oldVersion >= version) return cb(null, false)
      const ops = [
        { type: 'put', key: this._VERSION, value: version },
        { type: 'put', key: this._STATE, value: '' }
      ]
      this.db.batch(ops, err => cb(err, true))
    })
  }

  storeVersion (version, cb) {
    const value = String(version)
    this.db.put(this._VERSION, value, cb)
  }

  fetchVersion (cb) {
    getInt(this.db, this._VERSION, 0, cb)
  }
}

function getInt (db, key, defaultValue, cb) {
  db.get(key, (err, value) => {
    if (err && err.type !== 'NotFoundError') return cb(err)
    if (err) return cb(null, defaultValue)
    value = Number(value)
    cb(null, value)
  })
}

function noop () {}

class MemDB {
  constructor () {
    this.state = {}
  }

  put (key, value, cb) {
    this.state[key] = value
    cb()
  }

  del (key, cb) {
    this.state[key] = undefined
    cb()
  }

  get (key, cb) {
    if (typeof this.state[key] === 'undefined') {
      const err = new Error('Key not found')
      err.type = 'NotFoundError'
      err.notFound = true
      cb(err)
    } else {
      cb(null, this.state[key])
    }
  }

  batch (ops, cb) {
    ops.forEach(op => {
      if (op === 'put') this.put(op.key, op.value, noop)
      if (op === 'del') this.del(op.key, noop)
    })
    cb()
  }
}
