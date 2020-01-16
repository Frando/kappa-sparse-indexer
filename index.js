const sub = require('subleveldown')
const { Writable, Transform } = require('stream')
const mutex = require('mutexify')
const debug = require('debug')('indexer')
const pretty = require('pretty-hash')

const Log = require('./lib/log')
const State = require('./lib/state')

const KAPPA_MAX_BATCH = 50

module.exports = class Indexer {
  constructor (opts) {
    // The name is, at the moment, only for debug purposes.
    this.name = opts.name
    this.opts = opts

    this._statedb = sub(opts.db, 's')
    this._logdb = sub(opts.db, 'l')

    this.log = new Log(this._logdb, this.name)

    this.maxBatch = opts.maxBatch || 50
    this._feeds = []
    this._watchers = []
    this._lock = mutex()

    if (typeof opts.loadValue !== 'undefined') this._loadValue = opts.loadValue
  }

  ready (cb) {
    this._lock(release => {
      release(cb)
    })
  }

  watch (fn) {
    this._watchers.push(fn)
  }

  feed (key) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    return this._feeds[key]
  }

  add (feed, opts = {}, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    feed.ready(() => {
      this.addReady(feed, opts)
      if (cb) cb()
    })
  }

  addReady (feed, opts = {}) {
    const key = feed.key.toString('hex')
    if (this._feeds[key]) return
    this._feeds[key] = feed
    if (feed.writable) feed.on('append', () => this._onappend(feed))
    else feed.on('download', (seq) => this._ondownload(feed, seq))
    debug('[%s] feed %s (scan: %s)', this.name, pretty(key), !!opts.scan)

    if (opts.scan) {
      this._scan(feed)
    }
  }

  _scan (feed) {
    const self = this
    const key = feed.key.toString('hex')
    let max = feed.length
    let pending = 1
    for (let i = 0; i < max; i++) {
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

  createReadStream (opts = {}) {
    const { start = 0, end = Infinity } = opts
    return this.log.createReadStream(start, end)
      .pipe(this.createLoadStream())
  }

  pull (start, end, opts = {}, next) {
    if (typeof opts === 'function') {
      next = opts
      opts = {}
    }

    if (end <= start) return next()
    this.log.head((err, head) => {
      if (err) return next()
      if (start > head) return next()
      const to = Math.min(end, head)
      this.log.read(start, to, (err, messages) => {
        // debug('[%s] pull: from %s to %s: %s msgs [@%s]', this.name, start, to, messages.length, head)
        if (err) return next()
        this._transform(messages, opts, messages => {
          next({
            messages,
            head,
            seq: to
          })
        })
      })
    })
  }

  _transform (messages, opts, next) {
    const self = this
    if (!messages.length) return next(messages)
    if (opts.filterKey) messages = messages.filter(m => opts.filterKey(m.key))
    if (this.opts.loadValue === false) return next(messages)
    const _messages = messages
    messages = []
    let pending = _messages.length
    _messages.forEach(msg => this.loadValue(msg, done))
    function done (err, msg) {
      if (err) {}
      if (msg) messages.push(msg)
      if (--pending !== 0) return
      if (self.opts.transform) self.opts.transform(messages, next)
      else next(messages)
    }
  }

  head (cb) {
    this.log.head(cb)
  }

  // This can be overridden with opts.loadValue.
  // If the materialized log contains feeds not in the active set
  // this is required to return their values.
  _loadValueFromOpenFeeds (key, seq, cb) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    const feed = this.feed(key)
    if (!feed) return cb()
    feed.get(seq, { wait: false }, cb)
  }

  loadValue (message, cb) {
    if (this.opts.loadValue) return this.opts.loadValue(message.key, message.seq, cb)
    this._loadValueFromOpenFeeds(message.key, message.seq, (err, value) => {
      if (err) return cb(err)
      message.value = value
      cb(null, message)
    })
  }

  createLoadStream () {
    const self = this
    return new Transform({
      objectMode: true,
      transform (row, enc, next) {
        self.loadValue(row, (err, row) => {
          if (err) {} // TODO: Handle?
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
    this.opts = opts
    if (!this.opts.maxBatch) this.opts.maxBatch = KAPPA_MAX_BATCH
  }

  ready (cb) {
    this.idx.ready(cb)
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
      const end = lastseq + this.opts.maxBatch
      const filterKey = this.opts.filterKey
      this.idx.pull(lastseq + 1, end, { filterKey }, (result) => {
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

  get reset () { return this.state.reset }
  get storeVersion () { return this.state.storeVersion }
  get fetchVersion () { return this.state.fetchVersion }
  get api () {
    const self = this
    return {
      ready (kappa, cb) {
        self.ready(cb)
      },
      indexer: self.idx
    }
  }
}

function hex (key) {
  if (Buffer.isBuffer(key)) return key.toString('hex')
  return key
}
