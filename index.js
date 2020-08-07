const sub = require('subleveldown')
const { EventEmitter } = require('events')
const { Transform, PassThrough } = require('stream')
const mutex = require('mutexify')
const debug = require('debug')('indexer')
const pretty = require('pretty-hash')
const collect = require('stream-collector')

const Log = require('./lib/log')
const State = require('./lib/state')

const DEFAULT_MAX_BATCH = 50

module.exports = class Indexer extends EventEmitter {
  constructor (opts) {
    super()
    // The name is, at the moment, only for debug purposes.
    this.name = opts.name
    this.opts = opts

    const statedb = sub(opts.db, 's')
    const logdb = sub(opts.db, 'l')

    let loadValue = opts.loadValue
    if (loadValue !== false && typeof loadValue !== 'function') {
      loadValue = this._loadValueFromOpenFeeds.bind(this)
    }

    this._log = opts.log || new Log(logdb, { name: this.name, loadValue })
    this._state = new State(statedb)

    this._feeds = []
    this._downloadQueue = []
    this._lock = mutex()
    this._subscriptions = {}

    this._log.watch(() => this.emit('update'))

    // TODO: This is async, should it be awaited somewhere?
    this.open(noop)
  }

  createSubscription (name, opts = {}) {
    if (!this._subscriptions[name]) {
      if (opts.persist !== false && !opts.state) {
        opts.state = this._state.prefix(name)
      }
      opts.name = name
      this._subscriptions[name] = new Subscription(this, opts)
    }
    return this._subscriptions[name]
  }

  source (opts) {
    return new IndexerKappaSource(this, opts)
  }

  createSource (opts) {
    return this.source(opts)
  }

  open (cb) {
    this._log.open(cb)
  }

  ready (cb) {
    this.sync(cb)
  }

  sync (cb) {
    // Acquire a lock so that running ops are finished, and release right away.
    this._lock(release => {
      release(cb)
    })
  }

  get length () {
    return this._log.length
  }

  watch (fn) {
    this._log.watch(fn)
  }

  createReadStream (opts) {
    return this._log.createReadStream(opts)
  }

  read (opts, cb) {
    this._log.read(opts, cb)
  }

  head (cb) {
    return this._log.head(cb)
  }

  lseqToKeyseq (lseq, cb) {
    this._log.lseqToKeyseq(lseq, cb)
  }

  keyseqToLseq (key, seq, cb) {
    this._log.keyseqToLseq(key, seq, cb)
  }

  createLoadStream (opts) {
    return this._log.createLoadStream(opts)
  }

  /// Hypercore specific methods, thinking about moving these to a subclass.
  feed (key) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    return this._feeds[key]
  }

  add (feed, opts) {
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
      this._log.appendFlush(rows, release)
    })
  }

  _onappend (feed) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      const feedLen = feed.length
      this._log.keyhead(key, (err, indexedLen) => {
        if (err || indexedLen === undefined) indexedLen = -1
        const rows = range(indexedLen + 1, feedLen).map(seq => ({ key, seq }))
        this._log.appendFlush(rows, release)
      })
    })
  }

  _ondownload (feed, seq, _data) {
    const key = feed.key.toString('hex')
    this._downloadQueue.push({ key, seq })
    this._lock(release => {
      if (!this._downloadQueue.length) return release()
      const rows = this._downloadQueue
      this._downloadQueue = []
      this._log.appendFlush(rows, release)
    })
  }

  // This can be overridden with opts.loadValue.
  // If the materialized log contains feeds not in the active set
  // this is required to return their values.
  _loadValueFromOpenFeeds (message, next) {
    const { key, seq } = message
    const feed = this.feed(key)
    if (!feed) return next(message)
    feed.get(seq, { wait: false }, (err, value) => {
      // TODO: Handle error somehow?
      if (err) return next(message)
      next({ ...message, value })
    })
  }
}

class Subscription {
  constructor (source, opts) {
    this.source = source
    this.state = opts.state || new State(opts.db || null)
    this.name = opts.name
    this.opts = {
      limit: opts.limit || opts.maxBatch || DEFAULT_MAX_BATCH
    }
    if (opts.filterKey) this.opts.filterKey = opts.filterKey
    if (opts.loadValue) this.opts.loadValue = opts.loadValue
  }

  watch (fn) {
    return this.source.watch(fn)
  }

  sync (fn) {
    if (this.source.sync) this.source.sync(fn)
    else fn()
  }

  setCursor (seq, cb) {
    this.state.put(seq, cb)
  }

  setVersion (version, cb) {
    this.state.putVersion(version, cb)
  }

  read (opts, next) {
    this.source.read(opts, next)
  }

  getState (cb) {
    const info = {
      totalBlocks: this.source.length
    }
    let pending = 2
    this.state.get((err, state) => {
      if (err) return cb(err)
      info.indexedBlocks = state
      if (--pending === 0) cb(null, info)
    })
    this.state.fetchVersion((err, version) => {
      if (err) return cb(err)
      info.version = version
      if (--pending === 0) cb(null, info)
    })
  }

  createReadStream (opts) {
    return this.source.createReadStream(opts)
  }

  pull (opts, next) {
    if (typeof opts === 'function') return this.pull({}, opts)
    this.state.get((err, cursor) => {
      if (err) cursor = 0
      const readOpts = { ...this.opts, ...opts, start: cursor + 1 }
      this.read(readOpts, (err, messages) => {
        const result = {
          messages,
          finished: true,
          cursor
        }
        if (!err && messages.length) {
          result.cursor = messages[messages.length - 1].lseq
          result.finished = result.cursor >= result.head
        }
        if (err) return next({ ...result, error: err })
        result.ack = cb => this.setCursor(result.cursor, cb)
        next(result)
      })
    })
  }

  createPullStream (opts = {}) {
    const proxy = PassThrough({ objectMode: true })
    this.state.get((err, cursor) => {
      if (err) cursor = 0
      const readOpts = { ...this.opts, limit: Infinity, ...opts, start: cursor + 1 }
      this.createReadStream(readOpts).pipe(proxy)
    })
    proxy.ack = (cursor, cb) => this.setCursor(cursor, cb)
    return proxy
  }
}

class IndexerKappaSource {
  constructor (indexer, opts) {
    this.idx = indexer
    this.opts = opts
  }

  ready (cb) {
    this.idx.ready(cb)
  }

  open (flow, next) {
    this.name = flow.name
    this.subscription = this.idx.createSubscription('kappa:' + this.name, this.opts)
    this.subscription.watch(() => flow.update())
    this.reset = this.subscription.state.reset
    this.storeVersion = this.subscription.state.storeVersion
    this.fetchVersion = this.subscription.state.fetchVersion
    next()
  }

  pull (next) {
    this.subscription.pull(result => {
      next({
        ...result,
        onindexed: cb => result.ack(cb)
      })
    })
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
