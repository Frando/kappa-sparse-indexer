const sub = require('subleveldown')
const { EventEmitter } = require('events')
const { Transform, PassThrough } = require('stream')
const mutex = require('mutexify')
const debug = require('debug')('indexer')
const pretty = require('pretty-hash')
const collect = require('stream-collector')
const Nanoresource = require('nanoresource/emitter')

const Log = require('./lib/log')
const State = require('./lib/state')

const DEFAULT_MAX_BATCH = 50

class NotFoundError extends Error {
  constructor (message) {
    super(message)
    this.notFound = true
    this.code = 'ENOENT'
  }
}

function HypercoreIndexer (opts = {}) {
  if (opts.loadValue !== false && !opts.loadValue) {
    opts.loadValue = (...args) => {
      feedWatcher._loadValueFromOpenFeeds(...args)
    }
  }
  const indexer = new Indexer(opts)
  const feedWatcher = new HypercoreWatcher(indexer)
  indexer.feed = feedWatcher.feed.bind(feedWatcher)
  indexer.add = feedWatcher.add.bind(feedWatcher)
  indexer.addReady = feedWatcher.addReady.bind(feedWatcher)
  return indexer
}

class Indexer extends Nanoresource {
  constructor (opts) {
    super()
    // The name is, at the moment, only for debug purposes.
    this.name = opts.name
    this.opts = opts

    const statedb = sub(opts.db, 's')
    const logdb = sub(opts.db, 'l')

    const loadValue = opts.loadValue

    this._log = opts.log || new Log(logdb, { name: this.name, loadValue })
    this._state = new State(statedb)

    this._lock = mutex()
    this._subscriptions = {}

    this._log.watch(() => this.emit('update'))

    // TODO: This is async, should it be awaited somewhere?
    this.open(noop)
  }

  createSubscription (name, opts = {}) {
    if (!this._subscriptions[name]) {
      opts.name = name
      if (opts.persist !== false && !opts.state) {
        opts.state = this._state.prefix(name)
      }
      this._subscriptions[name] = new Subscription(this, opts)
    }
    return this._subscriptions[name]
  }

  createSource (opts) {
    return new IndexerKappaSource(this, opts)
  }

  createInputView () {
    const self = this
    return {
      map (messages, next) {
        const rows = messages.map(message => {
          let key
          if (isNaN(message.seq)) return null
          const seq = +message.seq
          if (Buffer.isBuffer(message.key)) key = message.key.toString('hex')
          if (typeof message.key === 'string') key = message.key
          if (!key) return null
          return { key, seq }
        }).filter(message => message)

        self.append(rows, next)
      }
    }
  }

  _open (cb) {
    this._log.open(cb)
  }

  _close (cb) {
    this._lock(release => {
      for (const subscription of Object.values(this._subscriptions)) {
        subscription.close()
      }
      release(cb)
    })
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

  keyhead (key, cb) {
    this._log.keyhead(key, cb)
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

  append (rows, cb) {
    this._lock(release => {
      this._log.appendFlush(rows, err => {
        release(cb, err)
      })
    })
  }

  appendUnlocked (rows, cb) {
    this._log.appendFlush(rows, cb)
  }

  lock (cb, ...args) {
    this._lock(cb, ...args)
  }

  resolveBlock (req, cb) {
    if (!empty(req.lseq) && empty(req.seq)) {
      this.lseqToKeyseq(req.lseq, (err, keyseq) => {
        if (!err && keyseq) {
          req.key = keyseq.key
          req.seq = keyseq.seq
        }
        finish(req)
      })
    } else if (empty(req.lseq)) {
      this.keyseqToLseq(req.key, req.seq, (err, lseq) => {
        if (!err && lseq) req.lseq = lseq
        finish(req)
      })
    } else finish(req)

    function finish (req) {
      if (empty(req.key) || empty(req.seq)) return cb(new NotFoundError('Block not found'))
      req.seq = parseInt(req.seq)
      if (!empty(req.lseq)) req.lseq = parseInt(req.lseq)
      if (Buffer.isBuffer(req.key)) req.key = req.key.toString('hex')
      cb(null, req)
    }
  }
}

class HypercoreWatcher {
  constructor (indexer) {
    this.indexer = indexer
    this._feeds = {}
    this._downloadQueue = []
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

    debug('[%s] add feed %s (scan: %s)', this.name, pretty(key), !!opts.scan)

    if (opts.scan) {
      this._scan(feed)
    }
  }

  _scan (feed) {
    const self = this
    const key = feed.key.toString('hex')
    const feedLen = feed.length

    if (!feedLen) return

    this._lock(release => {
      const rows = []
      // Native hypercores have a bitfield, which we can access synchronously.
      if (feed.bitfield) {
        onbitfield(feed.bitfield)
      // Remote hpercores don't (yet) have this bitfield, so we have to check
      // each seq manually.
      } else {
        scanManual()
      }

      function onbitfield (bitfield) {
        // debug('[%s] feed %s SCAN len %s', this.name, pretty(key), feedLen)
        for (let seq = 0; seq < feedLen; seq++) {
          if (bitfield.get(seq)) {
            rows.push({ key, seq })
          }
        }
        append()
      }

      // Scan manually (call has for each seq), asynchronously.
      // This is here for hyperspace compatibility until we can get
      // a feed's bitfield over the hyperspace RPC interface.
      function scanManual () {
        let pending = feedLen
        for (let seq = 0; seq < feedLen; seq++) {
          feed.has(seq, onhas.bind(null, seq))
        }
        function onhas (seq, err, has) {
          if (has) rows.push({ key, seq })
          if (--pending === 0) append()
        }
      }

      function append () {
        self.indexer.appendUnlocked(rows, release)
      }
    })
  }

  _lock (cb, ...args) {
    this.indexer.lock(cb, ...args)
  }

  _onappend (feed) {
    this._lock(release => {
      const key = feed.key.toString('hex')
      const feedLen = feed.length
      this.indexer.keyhead(key, (err, indexedLen) => {
        if (err || indexedLen === undefined) indexedLen = -1
        const rows = range(indexedLen + 1, feedLen).map(seq => ({ key, seq }))
        this.indexer.appendUnlocked(rows, release)
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
      this.indexer.appendUnlocked(rows, release)
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

  close () {
    if (this.closed) return
    this.closed = true
  }

  watch (fn) {
    return this.source.watch(fn)
  }

  sync (fn) {
    if (this.source.sync) this.source.sync(fn)
    else fn()
  }

  setCursor (seq, cb) {
    this.state.put(seq, err => {
      if (err) return cb(err)
      if (!cb) return
      cb(null, {
        indexedBlocks: seq,
        totalBlocks: this.source.length
      })
    })
  }

  setVersion (version, cb) {
    this.state.putVersion(version, cb)
  }

  read (opts, cb) {
    if (this.closed) return cb(new Error('Subscription closed'))
    this.source.read(opts, cb)
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
    if (this.closed) return next(new Error('Subscription closed'))
    this.state.get((err, cursor) => {
      if (err) cursor = 0
      const readOpts = { ...this.opts, ...opts, start: cursor + 1 }
      this.read(readOpts, (err, messages) => {
        if (err) return next(err)
        const result = {
          messages,
          finished: true,
          cursor
        }
        if (!err && messages.length) {
          result.cursor = messages[messages.length - 1].lseq
          result.finished = result.cursor >= result.head
        }
        result.ack = cb => this.setCursor(result.cursor, cb)
        next(null, result)
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
    this.indexer = indexer
    this.opts = opts
  }

  ready (cb) {
    this.indexer.ready(cb)
  }

  open (flow, next) {
    this.name = flow.name
    this.subscription = this.indexer.createSubscription('kappa:' + this.name, this.opts)
    this.subscription.watch(() => flow.update())
    this.reset = this.subscription.state.reset
    this.storeVersion = this.subscription.state.storeVersion
    this.fetchVersion = this.subscription.state.fetchVersion
    next()
  }

  pull (next) {
    this.subscription.pull((err, result) => {
      if (err) return next(err)
      next(null, result.messages, result.finished, result.ack)
    })
  }
}

function noop () {}

// from inclusive, to exclusive
function range (from, to) {
  const range = []
  if (!(to > from)) return range
  for (let i = from; i < to; i++) {
    range.push(i)
  }
  return range
}

function empty (value) {
  return value === undefined || value === null
}

module.exports = HypercoreIndexer
module.exports.Indexer = Indexer
module.exports.HypercoreWatcher = HypercoreWatcher
