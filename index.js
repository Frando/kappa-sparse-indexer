const sub = require('subleveldown')
const { Writable, Transform } = require('stream')
const mutex = require('mutexify')
const debug = require('debug')('indexer')

const Log = require('./lib/log')
const State = require('./lib/state')

module.exports = class Indexer {
  constructor (opts) {
    const { db } = opts
    this.name = opts.name
    this._statedb = sub(db, 's')
    this._logdb = sub(db, 'l')

    this.log = new Log(this._logdb, this.name)

    this.maxBatch = opts.maxBatch || 50
    this._feeds = []
    this._watchers = []
    this._lock = mutex()
  }

  watch (fn) {
    this._watchers.push(fn)
  }

  add (feed, opts = {}, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    feed.ready(() => {
      const key = feed.key.toString('hex')
      if (this._feeds[key]) return
      this._feeds[key] = feed
      if (feed.writable) feed.on('append', () => this._onappend(feed))
      else feed.on('download', (seq) => this._ondownload(feed, seq))
      debug('add feed %s (scan: %s)', key, opts.scan)

      if (opts.scan) {
        this._scan(feed)
      }
      if (cb) cb()
    })
  }

  _scan (feed) {
    const self = this
    const key = feed.key.toString('hex')
    let max = feed.length
    let pending = 1
    for (let i = 0; i <= max; i++) {
      if (!feed.bitfield.get(i)) {
        pending++
        this.log.add(key, i, done)
      }
    }
    done()
    function done () {
      if (--pending === 0) self.log.flush()
    }
  }

  _onappend (feed) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      const len = feed.length
      this.log.keyhead(key, (err, head) => {
        if (err) head = -1
        if (!(len > head)) return release()
        for (let i = head + 1; i < len; i++) {
          this.log.append(key, i)
        }
        this.log.flush(() => {
          release()
          this.onupdate()
        })
      })
    })
  }

  // TODO: Those can come in very fast. Possibly
  // collecting  the records while being locked
  // would be better.
  _ondownload (feed, seq, data) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      this.log.has(key, seq, (err, has) => {
        // TODO: Emit somewhere?
        if (err) {}
        if (!has) {
          this.log.append(key, seq)
          this.log.flush(() => {
            release()
            this.onupdate()
          })
        } else release()
      })
    })
  }

  onupdate () {
    this._watchers.forEach(fn => fn())
  }

  download (key, seq) {
    key = hex(key)
    seq = Number(seq)
    if (!this._feeds[key]) return
    this._feeds[key].download(seq)
  }

  createDownloadRequestStream () {
    const self = this
    return new Writable({
      objectMode: true,
      write (row, enc, next) {
        self.download(row.key, row.seq)
        next()
      }
    })
  }

  pull (start, end, next) {
    if (end <= start) return next()
    this.log.head((err, head) => {
      if (err) return next()
      if (start > head) return next()
      const to = Math.min(end, head)
      this.log.read(start, to, (err, messages) => {
        if (err) return next()
        next({
          messages,
          head,
          seq: to
        })
      })
    })
  }

  load (key, seq, cb) {
    key = hex(key)
    if (!this._feeds[key]) return cb(new Error('Feed unavailable: ' + key))
    this._feeds[key].get(seq, { wait: false }, cb)
  }

  createLoadStream () {
    const self = this
    return new Transform({
      objectMode: true,
      transform (row, enc, next) {
        self.load(row.key, row.seq, (err, value) => {
          if (err) {} // TODO: Handle?
          row.value = value
          this.push(row)
          next()
        })
      }
    })
  }

  source (opts) {
    return new IndexerSource(this, this._statedb, opts)
  }
}

class IndexerSource {
  constructor (idx, db, opts = {}) {
    this.idx = idx
    this.db = db
    this.maxBatch = opts.maxBatch || 50
  }

  open (flow, next) {
    this.name = flow.name
    this.state = new State(this.db, this.name)
    this.idx.watch(() => flow.update())
    next()
  }

  pull (next) {
    this.state.get((err, lastseq) => {
      if (err) return next()
      const end = lastseq + this.maxBatch
      this.idx.pull(lastseq + 1, end, (result) => {
        if (err || !result) return next()
        const { messages, seq, head } = result
        next({
          messages,
          finished: seq === head,
          onindexed: cb => this.state.put(seq, cb)
        })
      })
    })
  }

  transform (msgs, next) {
    if (!msgs.length) return next(msgs)
    let pending = msgs.length
    for (let i = 0; i < msgs.length; i++) {
      const msg = msgs[i]
      this.idx.load(msg.key, msg.seq, (err, value) => {
        // TODO: Emit somewhere?
        if (err) {}
        msg.value = value
        done()
      })
    }
    function done () {
      if (--pending === 0) next(msgs)
    }
  }

  get reset () { return this.state.reset }
  get storeVersion () { return this.state.storeVersion }
  get fetchVersion () { return this.state.fetchVersion }
  get api () {
    return {
      indexer: this.idx
    }
  }
}

function hex (key) {
  if (Buffer.isBuffer(key)) return key.toString('hex')
  return key
}
