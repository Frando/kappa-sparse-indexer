const sub = require('subleveldown')
const { Transform, PassThrough } = require('stream')
const mutex = require('mutexify')
const { EventEmitter } = require('events')
const debug = require('debug')('indexer')
const pretty = require('pretty-hash')
const collect = require('stream-collector')

const Log = require('./lib/log')
const State = require('./lib/state')

const DEFAULT_MAX_BATCH = 50

module.exports = class Indexer {
  constructor (opts) {
    // The name is, at the moment, only for debug purposes.
    this.name = opts.name
    this.opts = opts

    const statedb = sub(opts.db, 's')
    const logdb = sub(opts.db, 'l')

    this.log = new Log(logdb, this.name)
    this._state = new State(statedb, 's')

    this._feeds = []
    this._watchers = []
    this._lock = mutex()

    // TODO: This is async, should it be awaited somewhere?
    this.open(noop)
  }

  createSubscription (name, opts = {}) {
    opts.name = name
    if (opts.persist !== false && !opts.state) {
      opts.state = this._state.prefix(name)
    }
    return new Subscription(this, opts)
  }

  source (opts) {
    return new IndexerKappaSource(this, opts)
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

  appendFlush (rows, cb) {
    if (!rows.length) return cb()
    this.log.appendFlush(rows, () => {
      // debug('[%s] feed %s appended %s', this.name, pretty(key), seqs.join(', '))
      this.onupdate()
      cb()
    })
  }

  onupdate () {
    this._watchers.forEach(fn => fn(this.head()))
  }

  createReadStream (opts) {
    opts = this._expandOpts(opts)
    return this.log.createReadStream(opts)
      .pipe(this.createLoadStream(opts))
  }

  read (opts, next) {
    if (typeof opts === 'function') return this.read({}, opts)
    opts = this._expandOpts(opts)
    collect(this.createReadStream(opts), (err, messages) => {
      if (err) return next()
      next({
        messages,
        cursor: opts.end,
        finished: opts.end >= this.log.head(),
        head: this.log.head()
      })
    })
  }

  head (cb) {
    return this.log.head(cb)
  }

  lseqToKeyseq (lseq, cb) {
    this.log.lseqToKeyseq(lseq, cb)
  }

  keyseqToLseq (key, seq, cb) {
    this.log.keyseqToLseq(key, seq, cb)
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

  _expandOpts (opts = {}) {
    if (!opts.start) opts.start = 0
    if (!opts.end && opts.limit) opts.end = opts.start + opts.limit
    if (opts.end && opts.end > this.log.head()) opts.end = this.log.head()
    if (typeof opts.end === 'undefined') opts.end = this.log.head()
    return opts
  }

  _transformMessage (message, opts, next) {
    if (opts.filterKey && !opts.filterKey(message.key)) return next()
    pipeline(message, [
      (message, next) => this._loadValue(message, opts, next),
      this.opts.transform,
      opts.transform
    ], next)
  }

  _loadValue (message, opts, next) {
    const { key, seq } = message
    if (opts.loadValue === false) return next(message)
    if (opts.loadValue) opts.loadValue(key, seq, onvalue)
    else if (this.opts.loadValue) this.opts.loadValue(key, seq, onvalue)
    else if (this.opts.loadValue !== false) this._loadValueFromOpenFeeds(key, seq, onvalue)
    else next(message)

    function onvalue (err, value) {
      if (!err && value) message = { ...message, ...value }
      next(message)
    }
  }

  /// Hypercore specific methods, thinking about moving these to a subclass.
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

  _scan (feed) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      const feedLen = feed.length
      // debug('[%s] feed %s SCAN len %s', this.name, pretty(key), feedLen)
      const rows = []
      for (let seq = 0; seq < feedLen; seq++) {
        if (feed.bitfield.get(seq)) {
          rows.push({ key, seq })
        }
      }
      this.appendFlush(rows, release)
    })
  }

  _onappend (feed) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      const feedLen = feed.length
      this.log.keyhead(key, (err, indexedLen) => {
        if (err || indexedLen === undefined) indexedLen = -1
        const rows = range(indexedLen + 1, feedLen).map(seq => ({ key, seq }))
        this.appendFlush(rows, release)
      })
    })
  }

  // TODO: Those can come in very fast. Possibly
  // collecting  the records while being locked
  // would be better.
  _ondownload (feed, seq, data) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      const rows = [{ key, seq }]
      this.appendFlush(rows, release)
    })
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
}

class Subscription {
  constructor (source, opts) {
    this.source = source
    this.state = opts.state || new State(opts.db || null)
    this.name = opts.name
    this.opts = {
      filterKey: opts.filterKey,
      loadValue: opts.loadValue,
      transform: opts.transform,
      limit: opts.limit || opts.maxBatch || DEFAULT_MAX_BATCH
    }
  }

  watch (fn) {
    return this.source.watch(fn)
  }

  ready (fn) {
    this.source.ready(fn)
  }

  setCursor (seq, cb) {
    this.state.put(seq, cb)
  }

  setVersion (version, cb) {
    this.state.putVersion(version, cb)
  }

  read (opts, next) {
    if (typeof opts === 'function') return this.read({}, opts)
    this._expandOpts(opts, (err, opts) => {
      if (err) return next()
      this.source.read(opts, (result) => {
        if (!result) return next()
        next({
          ...result,
          ack: cb => this.setCursor(result.cursor, cb)
        })
      })
    })
  }

  createReadStream (opts = {}) {
    const proxy = PassThrough()
    this._expandOpts(opts, (err, opts) => {
      if (err) return proxy.destroy(err)
      this.source.createReadStream(opts).pipe(proxy)
    })
    return proxy
  }

  _expandOpts (opts, cb) {
    this.state.get((err, cursor) => {
      if (err) return cb(err)
      const readOpts = {
        ...this.opts,
        ...opts,
        start: cursor + 1
      }
      cb(null, readOpts)
    })
  }
}

class IndexerKappaSource {
  constructor (indexer, opts) {
    this.idx = indexer
    this.opts = opts
  }

  open (flow, next) {
    this.name = flow.name
    this.subscription = this.idx.createSubscription('kappa:' + this.name, this.opts)
    this.subscription.watch(() => flow.update())
    next()
  }

  pull (next) {
    this.subscription.read(result => {
      if (!result) return next()
      next({
        messages: result.messages,
        finished: result.finished,
        onindexed: cb => this.subscription.setCursor(result.cursor, cb)
      })
    })
  }

  reset () {
    this.subscription.state.reset()
  }
  storeVersion (version, cb) {
    this.subscription.state.storeVersion(version, cb)
  }
  fetchVersion (cb) {
    this.subscription.state.fetchVersion(cb)
  }

  get api () {
    const self = this
    return {
      ready (kappa, cb) {
        self.subscription.ready(cb)
      }
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

function pipeline (state, fns, final) {
  fns = fns.filter(fn => fn)
  next(state)
  function next (state) {
    const fn = fns.shift()
    if (!fn) return final(state)
    if (state === null) return final(state)
    process.nextTick(fn, state, next)
  }
}
