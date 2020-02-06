const { Transform } = require('stream')
const sub = require('subleveldown')
const collect = require('stream-collector')
const mutex = require('mutexify')

const LOG = 'l/'
const REV = 'r/'
const HEAD = 'head'

// A simple append-only log on a leveldb.
// Supports reverse lookup (value to seq)
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

  init (cb) {
    this.head((err, head) => {
      if (err) return cb(err)
      this._head = head
    })
  }

  append (val) {
    this._queue.push(val)
  }

  flush (cb) {
    if (!this._queue.length) return cb()
    this._lock(release => {
      const queue = this._queue
      this._queue = []
      this._filterQueue(queue, queue => {
        this.head((err, head) => {
          if (err) return release(cb, err)
          const ops = []
          for (const val of queue) {
            let seq = ++head
            let strseq = encodeInt(seq)
            ops.push({ type: 'put', key: LOG + strseq, value: val })
            ops.push({ type: 'put', key: REV + val, value: seq })
          }
          ops.push({ type: 'put', key: HEAD, value: head })
          this.db.batch(ops, (err) => {
            this._head = head
            release(cb, err)
          })
        })
      })
    })
  }

  _filterQueue (queue, next) {
    let pending = queue.length
    let filtered = []
    queue.forEach((value, i) => this.lookup(value, (_err, seq) => {
      if (seq === null) filtered.push(value)
      if (--pending === 0) next(filtered)
    }))
  }

  createReadStream (opts) {
    let { start, end } = opts
    // the log starts at 1, so negative numbers can never occur.
    if (start < 0) start = 0
    start = start ? encodeInt(start) : ''
    end = end ? encodeInt(end) : '\uFFFF'
    const lvlopts = {
      gte: LOG + start,
      lte: LOG + end
    }
    const transform = new Transform({
      objectMode: true,
      transform (row, enc, next) {
        const seq = row.key.split('/')[1]
        this.push({ seq: Number(seq), value: row.value })
        next()
      }
    })
    return this.db.createReadStream(lvlopts).pipe(transform)
  }

  lookup (value, cb) {
    this.db.get(REV + value, (err, seq) => {
      if (err) {}
      cb(null, seq ? Number(seq) : null)
    })
  }

  head (cb) {
    this.db.get(HEAD, (err, value) => {
      if (err && err.type !== 'NotFoundError') return cb(err)
      if (err) value = 0
      else value = Number(value)
      cb(null, value)
    })
  }
}

function encodeInt (seq) {
  let strseq = '' + seq
  strseq = '0'.repeat(9 - strseq.length) + strseq
  return strseq
}

// function decodeInt (str) {
//   return Number(str)
// }

class Heads {
  constructor (db) {
    this.db = db
  }

  get (key, cb) {
    this.db.get(key, (err, seq) => {
      if (err) seq = -1
      else seq = Number(seq)
      cb(null, seq)
    })
  }

  all (cb) {
    collect(this.db.createReadStream(), (err, res) => {
      if (err) return cb(err)
      res = res.reduce((agg, row) => {
        agg[row.key] = Number(row.value)
        return agg
      }, {})
      cb(null, res)
    })
  }

  put (key, seq, cb) {
    this.get(key, (err, oldseq) => {
      if (err) return cb(err)
      if (seq > oldseq) this.db.put(key, seq, cb)
      else cb()
    })
  }

  batch (heads, cb) {
    let pending = Object.keys(heads).length
    for (const [key, seq] of Object.entries(heads)) {
      this.put(key, seq, () => (--pending === 0 && cb()))
    }
  }
}

// TODO: More compact storage.
// This stores the 32 byte key, stringified into 64 byte utf8 hex,
// two times for each sequence number. that's 128B.
// The sequence numbers are also stored as string, 0 prefixed.
// so around 256B at least for each seq.
// This can be *much* more compact with a specialized storage
// (a map that maps each key to a varint, plus a proper integer storage)
// (integers as correctly sorted integers should be supported by level also)
// Likely this can even be done on top of leveldb.
// Or on top of bitfields.
module.exports = class MaterializedFeed {
  constructor (db, name) {
    // this._db = db
    this._name = name
    this._log = new Log(sub(db, 'l'))
    this._heads = new Heads(sub(db, 'h'))
    this._queue = []
    this._lock = mutex()
  }

  init (cb) {
    this._log.init(cb)
  }

  keyhead (key, cb) {
    this._heads.get(key, cb)
  }

  keyheads (cb) {
    this._heads.all(cb)
  }

  head (cb) {
    this._log.head(cb)
  }

  get length () {
    return this._log.length
  }

  createReadStream (start, end) {
    const transform = new Transform({
      objectMode: true,
      transform (chunk, enc, next) {
        const [key, seq] = chunk.value.split('@')
        const lseq = Number(chunk.seq)
        this.push({ key, seq: Number(seq), lseq })
        next()
      }
    })
    const rs = this._log.createReadStream({ start, end })
    return rs.pipe(transform)
  }

  read (start, end, cb) {
    collect(this.createReadStream(start, end), cb)
  }

  append (key, seq) {
    if (Array.isArray(key)) {
      key.forEach(([key, seq]) => this.append(key, seq))
      return
    }

    this._queue.push({ key, seq })
  }

  has (key, seq, cb) {
    return this._log.lookup(toFid(key, seq), cb)
  }

  flush (cb = noop) {
    if (!this._queue.length) return cb()
    this._lock(release => {
      const queue = this._queue
      this._queue = []

      const heads = {}
      for (const { key, seq } of queue) {
        const fid = toFid(key, seq)
        this._log.append(fid)
        if (!heads[key] || seq > heads[key]) heads[key] = seq
      }
      this._log.flush(() => {
        this._heads.batch(heads, () => {
          release(cb)
        })
      })
    })
  }
}

function toFid (key, seq) {
  key = hex(key)
  return key + '@' + seq
}

// function fromFid (fid) {
//   const [key, seq] = fid.split('@')
//   return { key, seq }
// }

function hex (key) {
  if (Buffer.isBuffer(key)) return key.toString('hex')
  return key
}

function noop () {}
