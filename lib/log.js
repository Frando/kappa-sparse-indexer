const { Transform } = require('stream')
const sub = require('subleveldown')
const collect = require('stream-collector')
const mutex = require('mutexify')
const thunky = require('thunky')
const varint = require('varint')

const LOG = Buffer.from('l')
const REV = Buffer.from('r')
const HEAD = Buffer.from('head')
const INFINITY = Buffer.alloc(4, 0xff)

// An ordered append-only set of values, backed by a leveldb.
// Supports reverse lookup (value to seq).
class Log {
  constructor (db) {
    this.db = db
    this._queue = []
    this._lock = mutex()
    this._head = undefined
  }

  get length () {
    return this._head
  }

  get head () {
    return this._head
  }

  open (cb) {
    this.db.get(HEAD, (err, value) => {
      if (err && err.type !== 'NotFoundError') return cb(err)
      if (err) this._head = 0
      else this._head = Number(value)
      cb()
    })
  }

  append (value) {
    if (!Buffer.isBuffer(value)) value = Buffer.from(value)
    this._queue.push(value)
  }

  appendFlush (value, cb) {
    this.append(value)
    this.flush(() => {
      this.lookup(value, cb)
    })
  }

  flush (cb) {
    const self = this
    this._lock(release => {
      if (!this._queue.length) return release(cb)
      const queue = this._queue
      this._queue = []

      let head = this._head
      let pending = queue.length
      const batch = this.db.batch()

      queue.forEach(value => {
        this.lookup(value, (err, existingseq) => {
          // Don't add values that are already in the log.
          if (!err && existingseq !== null) return done()

          const seq = ++head
          const bufseq = encodeInt(seq)
          batch.put(Buffer.concat([LOG, bufseq]), value)
          batch.put(Buffer.concat([REV, value]), bufseq)
          done()
        })
      })

      function done () {
        if (--pending !== 0) return
        batch.put(HEAD, head)
        batch.write(err => {
          self._head = head
          release(cb, err)
        })
      }
    })
  }

  createReadStream (opts) {
    let { start = 0, end = Infinity } = opts
    // the log starts at 1, so negative numbers can never occur.
    if (start < 0) start = 0
    const lvlopts = {
      gte: Buffer.concat([LOG, encodeInt(start)]),
      lte: Buffer.concat([LOG, encodeInt(end)])
    }
    const transform = new Transform({
      objectMode: true,
      transform (row, enc, next) {
        const bufseq = row.key.slice(1)
        const seq = decodeInt(bufseq)
        this.push({ seq, value: row.value })
        next()
      }
    })
    return this.db.createReadStream(lvlopts).pipe(transform)
  }

  get (seq, cb) {
    this.db.get(Buffer.concat([LOG, encodeInt(seq)]), (err, value) => {
      if (err) return cb(err)
      return cb(null, value)
    })
  }

  lookup (value, cb) {
    if (!Buffer.isBuffer(value)) value = Buffer.from(value)
    this.db.get(Buffer.concat([REV, value]), (err, seq) => {
      if (err) return cb(err)
      cb(null, decodeInt(seq))
    })
  }
}

function encodeInt (seq) {
  if (seq === Infinity) return Buffer.from(INFINITY)
  const buf = Buffer.alloc(4)
  buf.writeUInt32BE(seq)
  return buf
}

function decodeInt (buf) {
  if (!Buffer.isBuffer(buf)) buf = Buffer.from(buf)
  if (buf.equals(INFINITY)) return Infinity
  return buf.readUInt32BE()
}

class Keys {
  constructor (db) {
    this.db = db
    this._opened = false
    this._queue = {}
    this._keys = {}
    this._ids = {}
    this._heads = {}
  }

  open (cb) {
    let rs = this.db.createReadStream({ valueEncoding: 'binary' })
    this.idhead = 1
    rs.on('data', ({ key, value }) => {
      const { id, head } = this._decodeValue(value)
      this._keys[key] = id
      this._ids[id] = key
      this._heads[key] = head
      if (id > this.idhead) this.idhead = value
    })
    rs.on('end', () => {
      this._opened = true
      cb()
    })
    rs.once('error', cb)
  }

  shorten (key) {
    if (!this._opened) throw new Error('Key DB is not open')
    if (this._keys[key]) return this._keys[key]

    const id = ++this.idhead
    this._keys[key] = id
    this._ids[id] = key
    this._queue[key] = { id, head: 0 }
    return id
  }

  expand (shortkey) {
    if (!this._opened) throw new Error('Key DB is not open')
    if (this._ids[shortkey]) return this._ids[shortkey]
    return null
  }

  setHead (key, head) {
    if (this._heads[key] && this._heads[key] >= head) return
    const id = this.shorten(key)
    this._heads[key] = head
    this._queue[key] = { id, head }
  }

  setHeads (heads) {
    for (const [key, head] of Object.entries(heads)) {
      this.setHead(key, head)
    }
  }

  head (key) {
    if (typeof this._heads[key] === 'undefined') return -1
    return this._heads[key]
  }

  heads () {
    return { ...this._heads }
  }

  flush (cb) {
    const ops = Object.entries(this._queue).map(([key, { id, head }]) => (
      { key, value: this._encodeValue({ id, head }), type: 'put' }
    ))
    this._queue = {}
    this.db.batch(ops, cb)
  }

  _encodeValue ({ id, head }) {
    const buf = Buffer.alloc(8)
    buf.writeUInt32BE(id, 0)
    buf.writeUInt32BE(head, 4)
    return buf
  }

  _decodeValue (buf) {
    if (!Buffer.isBuffer(buf)) buf = Buffer.from(buf)
    const id = buf.readUInt32BE(0)
    const head = buf.readUInt32BE(4)
    return { id, head }
  }
}

module.exports = class MaterializedFeed {
  constructor (db, name) {
    this._name = name
    this._log = new Log(sub(db, 'l'))
    this._keys = new Keys(sub(db, 'k'))
    this._queue = []
    this._lock = mutex()
    this._opened = false
    this.open = thunky(this._open.bind(this))
  }

  _open (cb) {
    this._keys.open(() => {
      this._log.open(() => {
        this._opened = true
        cb()
      })
    })
  }

  keyhead (key, cb) {
    const head = this._keys.head(key)
    if (cb) cb(null, head)
    else return head
  }

  keyheads (cb) {
    const heads = this._keys.heads()
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
    const { start, end } = opts
    const self = this
    const transform = new Transform({
      objectMode: true,
      transform (row, _enc, next) {
        const { key, seq } = self._decodeValue(row.value)
        const lseq = row.seq
        this.push({ key, seq, lseq })
        next()
      }
    })
    const rs = this._log.createReadStream({ start, end })
    return rs.pipe(transform)
  }

  read (start, end, cb) {
    collect(this.createReadStream({ start, end }), cb)
  }

  append (key, seq) {
    if (Array.isArray(key)) {
      key.forEach(([key, seq]) => {
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

  has (key, seq, cb) {
    this.lookup({ key, seq }, (err, lseq) => cb(err, !!lseq))
  }

  get (lseq, cb) {
    this._log.get(lseq, (err, value) => {
      if (err) return cb(err)
      cb(err, this._decodeValue(value))
    })
  }

  lookup ({ key, seq }, cb) {
    const value = this._encodeValue({ key, seq })
    this._log.lookup(value, cb)
  }

  keyseqToLseq (key, seq, cb) {
    this.lookup({ key, seq }, cb)
  }

  lseqToKeyseq (lseq, cb) {
    this.get(lseq, cb)
  }

  flush (cb = noop) {
    if (!this._queue.length) return cb()
    if (!this._opened) return this.open(() => this.flush(cb))
    this._lock(release => {
      const queue = this._queue
      this._queue = []
      for (const { key, seq } of queue) {
        const value = this._encodeValue({ key, seq })
        this._log.append(value)
        this._keys.setHead(key, seq)
      }

      this._log.flush(() => {
        this._keys.flush(() => {
          release(cb)
        })
      })
    })
  }

  _decodeValue (buf) {
    if (!Buffer.isBuffer(buf)) buf = Buffer.from(buf)
    const keyid = varint.decode(buf)
    const seq = varint.decode(buf, varint.decode.bytes)
    const key = this._keys.expand(keyid)
    return { key, seq }
  }

  _encodeValue ({ key, seq }) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    const keyid = this._keys.shorten(key)
    const len = varint.encodingLength(keyid) + varint.encodingLength(seq)
    const buf = Buffer.alloc(len)
    varint.encode(keyid, buf)
    varint.encode(seq, buf, varint.encode.bytes)
    return buf
  }
}

function noop () {}
