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
    // TODO: This is async, should it be awaited somewhere?
    this.log.init(noop)

    this.maxBatch = opts.maxBatch || 50
    this._feeds = []
    this._watchers = []
    this._lock = mutex()
  }

  ready (cb) {
    // Acquire a lock so that running ops are finished, and release right away.
    this._lock(release => release(cb))
  }

  get length () {
    return this.log.length
  }

  watch (fn) {
    this._watchers.push(fn)
  }

  feed (key) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    return this._feeds[key]
  }

  add (feed, opts = {}) {
    if (feed.opened) {
      this.addReady(feed, opts)
    } else {
      feed.ready(() => this.addReady(feed, opts))
    }
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

  _appendAndFlush (key, seqs, cb) {
    if (!seqs.length) return cb()
    const rows = seqs.map(seq => [key, seq])
    this.log.append(rows)
    this.log.flush(() => {
      // debug('[%s] feed %s appended %s', this.name, pretty(key), seqs.join(', '))
      this.onupdate()
      cb()
    })
  }

  _scan (feed) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      const feedLen = feed.length
      // debug('[%s] feed %s SCAN len %s', this.name, pretty(key), feedLen)
      const seqs = []
      for (let seq = 0; seq < feedLen; seq++) {
        if (feed.bitfield.get(seq)) {
          seqs.push(seq)
        }
      }
      this._appendAndFlush(key, seqs, release)
    })
  }

  _onappend (feed) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      const feedLen = feed.length
      this.log.keyhead(key, (err, indexedLen) => {
        if (err) indexedLen = -1
        const seqs = range(indexedLen + 1, feedLen)
        this._appendAndFlush(key, seqs, release)
      })
    })
  }

  // TODO: Those can come in very fast. Possibly
  // collecting  the records while being locked
  // would be better.
  _ondownload (feed, seq, data) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      this._appendAndFlush(key, [seq], release)
    })
  }

  onupdate () {
    this._watchers.forEach(fn => fn())
  }

  createReadStream (opts = {}) {
    const { start = 0, end = Infinity } = opts
    const rs = this.log.createReadStream(start, end)
    if (opts.loadValue !== false) return rs.pipe(this.createLoadStream())
    return rs
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
    const results = []
    let pending = messages.length
    messages.forEach(msg => this.loadValue(msg, done))
    function done (err, msg) {
      if (err) {}
      if (msg) results.push(msg)
      if (--pending !== 0) return
      if (self.opts.transform) self.opts.transform(results, next)
      else next(results)
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
    feed.get(seq, { wait: false }, (err, value) => {
      if (err) return cb(err)
      cb(null, { value })
    })
  }

  loadValue (message, cb) {
    if (this.opts.loadValue) this.opts.loadValue(message.key, message.seq, onvalue)
    else this._loadValueFromOpenFeeds(message.key, message.seq, onvalue)

    function onvalue (err, value) {
      if (err) return cb(err)
      // TODO: This "monkey patches" the result from loadValue and the original
      // message object with { key, seq, lseq } together. I'm not totally sure
      // if that's what we want here, but it works well.
      cb(null, { ...message, ...value })
    }
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

function noop () {}

// from inclusive, to exclusive
function range (from, to) {
  let range = []
  if (!(to > from)) return range
  for (let i = from; i < to; i++) {
    range.push(i)
  }
  return range
}
