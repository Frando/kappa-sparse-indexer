const State = require('kappa-core/sources/util/state')
const sub = require('subleveldown')
const { Writable, Transform } = require('stream')
const Log = require('./lib/log')
const Lock = require('mutexify')
const debug = require('debug')('indexer')

module.exports = class Indexer {
  constructor (opts) {
    const { db } = opts
    this.name = opts.name
    this.state = new State({
      db: sub(db, 's')
    })
    this.log = new Log(sub(db, 'l'), this.name)
    this.maxBatch = opts.maxBatch || 50
    this._feeds = []
    this._listeners = []
    this._lock = Lock()
  }

  add (feed, opts = {}) {
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
    })
  }

  _scan (feed) {
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
      if (--pending === 0) this.log.flush()
    }
  }

  _onappend (feed) {
    this._lock(release => {
      // console.log('ONAPPEND', this.name)
      // Ignore onappend events for remote feeds.
      if (!feed.writable) return release()
      const key = feed.key.toString('hex')
      const seq = feed.length
      this.log.head(key, (err, last) => {
        if (err || !last) last = -1
        if (seq <= last) return release()
        for (let i = last + 1; i <= seq; i++) {
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
        if (err) {}
        if (!has) {
          this.log.append(key, seq)
          this.log.flush(() => {
            release()
            this.onupdate()
          })
        }
      })
    })
  }

  onupdate () {
    this._listeners.forEach(fn => fn())
  }

  download (key, seq) {
    key = hex(key)
    seq = Number(seq)
    // console.log('download', { key, seq })
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

  _pull (name, next) {
    this.state.get(name, (err, stateseq) => {
      if (err) return next()
      this.log.length((err, headseq) => {
        if (err || headseq === stateseq) return next()
        const to = Math.min(stateseq + this.maxBatch, headseq)
        this.log.read(stateseq, to, (err, messages) => {
          if (err) return next()
          next({
            messages,
            finished: to === headseq,
            onindexed: cb => this.state.put(name, to, cb)
          })
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

  source () {
    const self = this
    let name
    return {
      open (flow, next) {
        name = flow.name
        self._listeners.push(() => flow.update())
        next()
      },
      pull (next) {
        self._pull(name, next)
      },
      transform (msgs, next) {
        if (!msgs.length) return next(msgs)
        let pending = msgs.length
        for (let i = 0; i < msgs.length; i++) {
          const msg = msgs[i]
          self.load(msg.key, msg.seq, (err, value) => {
            if (err) {}
            msg.value = value
            done()
          })
        }
        function done () {
          if (--pending === 0) next(msgs)
        }
      },
      api: {
        indexer: self
      }
    }
  }
}

function hex (key) {
  if (Buffer.isBuffer(key)) return key.toString('hex')
  return key
}
