const sub = require('subleveldown')
const { Writable, Transform } = require('stream')
const mutex = require('mutexify')
const debug = require('debug')('indexer')
const pretty = require('pretty-hash')
const collect = require('stream-collector')

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
    this.open(noop)

    this.maxBatch = opts.maxBatch || 50
    this._feeds = []
    this._watchers = []
    this._lock = mutex()
  }

  open (cb) {
    this.log.open(cb)
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
    return () => (this._watchers = this._watchers.filter(f => f !== fn))
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
    return this.log.createReadStream(start, end)
      .pipe(this.createLoadStream(opts))
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
      end = Math.min(end, head)
      collect(this.createReadStream({ ...opts, start, end }), (err, messages) => {
        if (err) return next()
        next({
          messages,
          head,
          seq: end
        })
      })
    })
  }

  _transformMessage (message, opts, next) {
    if (opts.filterKey && !opts.filterKey(message.key)) return next()
    if (this.opts.loadValue === false) return next(message)
    this.loadValue(message, (err, message) => {
      if (err) {}
      if (this.opts.transform) this.opts.transform(message, next)
      else next(message)
    })
  }

  head (cb) {
    this.log.head(cb)
  }

  lseqToKeyseq (lseq, cb) {
    this.log.lseqToKeyseq(lseq, cb)
  }

  keyseqToLseq (key, seq, cb) {
    this.log.keyseqToLseq(key, seq, cb)
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

  createLoadStream (opts = {}) {
    const self = this
    return new Transform({
      objectMode: true,
      transform (row, enc, next) {
        self._transformMessage(row, opts, (row) => {
          if (row) this.push(row)
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
