module.exports = class SimpleState {
  constructor (db, prefix) {
    this.db = db || new MemDB()
    this._prefix = prefix || ''
    this._STATE = this._prefix + '!state!'
    this._VERSION = this._prefix + '!version!'
    // Bind public methods so that they can be passed on directly.
    this.get = this.get.bind(this)
    this.reset = this.reset.bind(this)
    this.put = this.put.bind(this)
    this.storeVersion = this.storeVersion.bind(this)
    this.fetchVersion = this.fetchVersion.bind(this)
  }

  prefix (prefix) {
    return new SimpleState({
      db: this.db,
      prefix: this._prefix + '/' + prefix
    })
  }

  reset (cb) {
    const ops = [this._STATE, this._VERSION].map(key => ({ key, type: 'del' }))
    this.db.batch(ops, cb)
  }

  get (cb) {
    getInt(this.db, this._STATE, -1, cb)
  }

  put (seq, cb = noop) {
    putInt(this.db, this._STATE, seq, cb)
  }

  storeVersion (version, cb) {
    putInt(this.db, this._VERSION, version, cb)
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

function putInt (db, key, int, cb) {
  const value = String(int)
  db.put(key, value, cb)
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
    delete this.state[key]
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
      if (op === 'del') this.put(op.key, noop)
    })
    cb()
  }
}
