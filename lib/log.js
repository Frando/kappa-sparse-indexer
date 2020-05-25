const { Transform, pipeline } = require('stream')
const sub = require('subleveldown')
const collect = require('stream-collector')
const mutex = require('mutexify')
const thunky = require('thunky')
const varint = require('varint')
const codecs = require('codecs')
const Live = require('level-live')
const Codec = require('level-codec')

const { transformPipeline } = require('./util')

const LOG = Buffer.from('l')
const REV = Buffer.from('r')
const HEAD = Buffer.from('head')
const UINT32_MAX = Buffer.alloc(4, 0xff)

// An ordered append-only set of values, backed by a leveldb.
// Supports reverse lookup (value to seq).
class Log {
  constructor (db, opts = {}) {
    this.db = db
    this._queue = []
    this._lock = mutex()
    this._head = undefined

    // opts
    this._lookup = opts.lookup === undefined ? true : opts.lookup
    this._deduplicate = this._lookup && opts.deduplicate
    this._watchers = []

    // Note: I'm not sure if there's a better way to change the db codec, but this works.
    this.db._db.codec = new Codec({
      keyEncoding: 'binary',
      valueEncoding: 'binary'
    })
  }

  get length () { return this._head }
  get head () { return this._head }

  watch (fn) {
    this._watchers.push(fn)
    return () => this._watchers.filter(f => f !== fn)
  }

  open (cb) {
    this.db.get(HEAD, (err, value) => {
      if (err && err.type !== 'NotFoundError') return cb(err)
      if (err) this._head = 0
      else this._head = decodeInt(value)
      cb()
    })
  }

  append (value) {
    this._queue.push(value)
  }

  appendFlush (value, cb) {
    this.append(value)
    this.flush(cb)
  }

  flush (cb) {
    const self = this
    this._lock(release => {
      if (!self._queue.length) return release(cb)
      const batch = []
      const queue = self._queue
      const results = []
      self._queue = []

      let head = self._head
      let pending = queue.length
      for (const value of queue) {
        if (!self._deduplicate) add(value)
        else addIfMissing(value)
      }

      function addIfMissing (value) {
        self.lookup(value, (err, existingseq) => {
          // Don't add values that are already in the log.
          if (!err && existingseq !== null) done()
          else add(value)
        })
      }

      function add (value) {
        const seq = ++head
        const bufseq = encodeInt(seq)
        batch.push({ type: 'put', key: Buffer.concat([LOG, bufseq]), value })
        if (self._lookup) {
          batch.push({ type: 'put', key: Buffer.concat([REV, value]), value: bufseq })
        }
        results.push({ seq, value })
        done()
      }

      function done () {
        if (--pending !== 0) return
        batch.push({ type: 'put', key: HEAD, value: encodeInt(head) })
        self.db.batch(batch, err => {
          if (err) return release(cb, err)
          self._head = head
          self._watchers.forEach(fn => {
            fn(results)
          })
          release(cb, null, head)
        })
      }
    })
  }

  createReadStream (opts) {
    let { start, end, limit, live } = opts
    // the log starts at 1, so negative numbers can never occur.
    if (!start || start < 0) start = 0
    if (end === undefined && limit) end = start + limit
    if (end === undefined) end = Infinity
    const lvlopts = {
      gte: Buffer.concat([LOG, encodeInt(start)]),
      lte: Buffer.concat([LOG, encodeInt(end)])
    }
    const transform = new Transform({
      objectMode: true,
      transform (row, _enc, next) {
        const bufseq = row.key.slice(1)
        const seq = decodeInt(bufseq)
        this.push({ seq, value: row.value })
        next()
      }
    })
    if (!live) {
      return this.db.createReadStream(lvlopts).pipe(transform)
    } else {
      return new Live(this.db, lvlopts).pipe(transform)
    }
  }

  get (seq, cb) {
    this.db.get(Buffer.concat([LOG, encodeInt(seq)]), (err, value) => {
      if (err) return cb(err)
      return cb(null, value)
    })
  }

  lookup (value, cb) {
    if (!this._lookup) return cb(new Error('Lookup is disabled'))
    this.db.get(Buffer.concat([REV, value]), (err, buf) => {
      if (err) return cb(err)
      const seq = decodeInt(buf)
      cb(null, seq)
    })
  }
}

// A KV store backed by leveldb that assigns each key a unique
// int id, and that loads all data on open so that further ops
// are sync (apart from the required flush to commit changes).
class KVSet {
  constructor (db, opts = {}) {
    this.db = db
    this._queue = {}
    this._ids = {}
    this._data = {}
    this._condition = opts.condition
    this._opened = false
    this._idhead = 0
    const valueEncoding = codecs(opts.valueEncoding || 'binary')
    this._encoding = UInt32PrefixEncoding(valueEncoding)
  }

  open (cb) {
    const rs = this.db.createReadStream({ valueEncoding: 'binary' })
    collect(rs, (err, nodes) => {
      if (err) return cb(err)
      for (const node of nodes) {
        const { id, value } = this._encoding.decode(node.value)
        this._put(id, node.key, value, false)
        if (id > this._idhead) this._idhead = id
      }
      this._opened = true
      cb()
    })
  }

  put (key, value, opts = {}) {
    const condition = opts.condition || this._condition
    let id
    if (!this._data[key]) {
      id = ++this._idhead
      this._put(id, key, value, true)
    } else {
      id = this._data[key].id
      const oldValue = this._data[key].value
      if (!condition || (oldValue === undefined || condition(oldValue, value))) {
        this._put(id, key, value, true)
      }
    }
    return id
  }

  _put (id, key, value, update) {
    this._data[key] = { id, value }
    this._ids[id] = key
    if (update) this._queue[key] = true
  }

  get (key) {
    if (this._data[key]) return this._data[key].value
  }

  lookup (id) {
    return this._ids[id]
  }

  list () {
    return Object.keys(this._data)
  }

  flush (cb) {
    const ops = Object.keys(this._queue).map(key => {
      const value = this._encoding.encode(this._data[key])
      return { type: 'put', key, value }
    })
    this._queue = {}
    this.db.batch(ops, cb)
  }
}

// An append-only log of keyed logs, backed by leveldb.
module.exports = class KeyedLog {
  constructor (db, opts = {}) {
    const logdb = sub(db, 'l')
    const keydb = sub(db, 'k')
    this._log = new Log(logdb, {
      lookup: true,
      deduplicate: true
    })
    this._keys = new KVSet(keydb, {
      valueEncoding: UInt32Encoding,
      condition (oldValue, newValue) {
        return newValue > oldValue
      }
    })
    this._name = opts.name
    this._opts = opts
    this._queue = []
    this._lock = mutex()
    this._opened = false
    this.open = thunky(this._open.bind(this))

    this._watchers = []
  }

  _open (cb) {
    this._keys.open(() => {
      this._log.open(() => {
        this._opened = true
        cb()
      })
    })
  }

  watch (fn) {
    this._watchers.push(fn)
    return () => (this._watchers = this._watchers.filter(f => f !== fn))
  }

  keyhead (key, cb) {
    const head = this._keys.get(key)
    if (cb) cb(null, head)
    else return head
  }

  keyheads (cb) {
    const heads = {}
    for (const key of this._keys.list()) {
      heads[key] = this._keys.get(key)
    }
    if (cb) cb(null, heads)
    else return heads
  }

  head (cb) {
    const head = this._log.head
    if (cb) cb(null, head)
    else return head
  }

  get length () {
    return this._log.length
  }

  createReadStream (opts = {}) {
    const self = this
    const rs = this._log.createReadStream(opts)
    const transform = new Transform({
      objectMode: true,
      transform (row, _enc, next) {
        const message = self._decodeValue(row.value)
        // message = { key, seq }
        message.lseq = row.seq
        this.cursor = row.seq
        self._onread(message, opts, message => {
          if (message !== null) this.push(message)
          next()
        })
      }
    })

    return pipeline(rs, transform, err => {
      if (err) transform.emit('error', err)
    })
  }

  read (opts, cb) {
    if (typeof opts === 'function') return this.read({}, opts)
    collect(this.createReadStream(opts), cb)
  }

  createLoadStream (opts = {}) {
    const self = this
    return new Transform({
      objectMode: true,
      transform (row, _enc, next) {
        self._onread(row, opts, (row) => {
          if (row !== null) this.push(row)
          next()
        })
      }
    })
  }

  _onread (message, opts, next) {
    if (Buffer.isBuffer(message.key)) message.key = message.key.toString('hex')

    if (opts.filterKey && !opts.filterKey(message.key)) return next(null)

    if (opts.load !== false) {
      if (opts.loadValue) return opts.loadValue(message, next)
      if (this._opts.loadValue) return this._opts.loadValue(message, next)
    }

    return next(message)
  }

  has (key, seq, cb) {
    this.lookup({ key, seq }, (err, lseq) => cb(err, !!lseq))
  }

  get (lseq, opts, cb) {
    if (typeof opts === 'function') return this.get(lseq, {}, opts)
    this._log.get(lseq, (err, value) => {
      if (err) return cb(err)
      const { key, seq } = this._decodeValue(value)
      this._onread({ key, seq, lseq }, opts, row => {
        cb(null, row)
      })
    })
  }

  lookup ({ key, seq }, cb) {
    const value = this._encodeValue({ key, seq })
    this._log.lookup(value, cb)
  }

  append (key, seq) {
    if (Array.isArray(key)) {
      key.forEach(({ key, seq }) => {
        this._queue.push({ key, seq })
      })
    } else {
      this._queue.push({ key, seq })
    }
  }

  appendFlush (rows, cb) {
    this.append(rows)
    this.flush(cb)
  }

  keyseqToLseq (key, seq, cb) {
    this.lookup({ key, seq }, cb)
  }

  lseqToKeyseq (lseq, cb) {
    this.get(lseq, cb)
  }

  sync (cb) {
    this._lock(release => release(cb))
  }

  flush (cb) {
    if (!this._queue.length) return cb()
    if (!this._opened) return this.open(() => this.flush(cb))
    this._lock(release => {
      const queue = this._queue
      this._queue = []
      for (const { key, seq } of queue) {
        const value = this._encodeValue({ key, seq })
        this._log.append(value)
        this._keys.put(key, seq)
      }

      this._keys.flush(() => {
        this._log.flush(() => {
          this._watchers.forEach(fn => fn())
          release(cb)
        })
      })
    })
  }

  _decodeValue (buf) {
    const [keyid, seq] = VarintArrayEncoding.decode(buf)
    const key = this._keys.lookup(keyid)
    return { key, seq }
  }

  _encodeValue ({ key, seq }) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    const keyid = this._keys.put(key)
    return VarintArrayEncoding.encode([keyid, seq])
  }
}

function encodeInt (seq) {
  if (seq === Infinity) return UINT32_MAX
  const buf = Buffer.alloc(4)
  buf.writeUInt32BE(seq)
  return buf
}

function decodeInt (buf) {
  if (!Buffer.isBuffer(buf)) buf = Buffer.from(buf)
  return buf.readUInt32BE()
}

const UInt32Encoding = {
  encode (num) {
    return encodeInt(num)
  },
  decode (buf) {
    return decodeInt(buf)
  }
}

function UInt32PrefixEncoding (valueEncoding) {
  return {
    encode (node) {
      const buf = UInt32Encoding.encode(node.id)
      if (node.value !== undefined) {
        const valueBuf = valueEncoding.encode(node.value)
        return Buffer.concat([buf, valueBuf])
      }
      return buf
    },
    decode (buf) {
      const id = UInt32Encoding.decode(buf)
      let value
      if (buf.length > 4) {
        value = valueEncoding.decode(buf.slice(4))
      }
      return { id, value }
    }
  }
}

const VarintArrayEncoding = {
  encode (values) {
    const len = values.reduce((val, cur) => val + varint.encodingLength(cur), 0)
    const buf = Buffer.alloc(len)
    let offset = 0
    for (const value of values) {
      varint.encode(value, buf, offset)
      offset += varint.encode.bytes
    }
    return buf
  },
  decode (buf) {
    const len = buf.length
    const values = []
    let offset = 0
    while (offset < len) {
      values.push(varint.decode(buf, offset))
      offset += varint.decode.bytes
    }
    return values
  }
}
