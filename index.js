const State = require('kappa-core/sources/util/state')
const sub = require('subleveldown')
const Log = require('./lib/log')
const { Writable } = require('stream')

module.exports = class Indexer {
  constructor (opts) {
    const { db } = opts
    this.state = new State({
      db: sub(db, 's')
    })
    this.log = new Log(sub(db, 'l'))
    this.maxBatch = opts.maxBatch || 50
    this._feeds = []
    this._listeners = []
  }

  add (feed, opts = {}) {
    feed.ready(() => {
      const key = feed.key.toString('hex')
      if (this._feeds[key]) return
      this._feeds[key] = feed
      feed.on('append', () => this._onappend(feed))
      feed.on('download', (seq) => this._ondownload(feed, seq))

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
    const key = feed.key.toString('hex')
    const seq = feed.length
    this.log.head(key, (err, last) => {
      if (err || !last) last = -1
      if (seq <= last) return
      for (let i = last + 1; i <= seq; i++) {
        this.log.append(key, i)
      }
      this.log.flush()
      this.onupdate(key)
    })
  }

  _ondownload (feed, seq, data) {
    const key = feed.key.toString('hex')
    this.log.has(key, seq, (err, has) => {
      if (err || !has) {
        this.log.append(key, seq)
        this.log.flush()
        this.onupdate()
      }
    })
  }

  onupdate () {
    this._listeners.forEach(fn => fn())
  }

  download (key, seq) {
    key = hex(key)
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
    if (!this._feeds[key]) return cb(new Error('Feed unavailable: ' + key))
    this._feeds[key].get(seq, cb)
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
        let pending = msgs.length - 1
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
      }
    }
  }
}

function hex (key) {
  if (Buffer.isBuffer(key)) return key.toString('hex')
  return key
}
