const { Transform } = require('stream')

module.exports = {
  createTopicsView,
  createRecentView
}

function createTopicsView (db) {
  return {
    filter (msgs, next) {
      next(msgs.filter(msg => msg.value && msg.value.topics))
    },
    map (msgs, next) {
      const ops = msgs.reduce((agg, msg) => {
        return agg.concat(msg.value.topics.map(topic => ({
          type: 'put',
          key: topic + '/' + msg.key + '@' + msg.seq,
          value: ''
        })))
      }, [])
      db.batch(ops, next)
    },
    api: {
      query (kappa, query) {
        const { topic } = query
        const opts = { gte: topic + '/', lt: topic + '/' + '\uFFFF' }
        return db.createReadStream(opts).pipe(splitKeyseqStream())
      }
    }
  }
}

function createRecentView (db) {
  return {
    filter (msgs, next) {
      next(msgs.filter(msg => msg.value && msg.value.timestamp))
    },
    map (msgs, next) {
      const ops = msgs.map(msg => ({
        type: 'put',
        key: msg.value.timestamp + '/' + msg.key + '@' + msg.seq,
        value: ''
      }))
      db.batch(ops, next)
    },
    api: {
      query (kappa, query) {
        const { timestamp } = query
        const opts = { gt: timestamp + '/', lt: timestamp + '/' + '\uFFFF' }
        return db.createReadStream(opts).pipe(splitKeyseqStream())
      }
    }
  }
}

function splitKeyseqStream () {
  return new Transform({
    objectMode: true,
    transform (msg, enc, next) {
      const keyseq = msg.key.split('/')[1]
      const [key, seq] = keyseq.split('@')
      this.push({ key, seq: Number(seq) })
      next()
    }
  })
}
