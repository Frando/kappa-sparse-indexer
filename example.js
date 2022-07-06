const multifeed = require('multifeed')
const Query = require('hypercore-query-extension')
const Kappa = require('@frando/kappa-core')
const Indexer = require('.')
const sub = require('subleveldown')
const hypercore = require('hypercore')
const mem = require('level-mem')
const { Transform, Writable } = require('stream')
const collect = require('stream-collector')
const ram = require('random-access-memory')
const pretty = require('pretty-hash')

// example
const peer1 = createApp('p1')
const peer2 = createApp('p2')
ready(peer1, peer2, example)
function example () {
  // replicate both peers in live mode.
  replicate(peer1, peer2)

  // append some data on peer1.
  peer1.feeds.writer((err, feed) => {
    if (err) return console.error(err)
    feed.append([
      {
        name: 'first',
        timestamp: Date.now(),
        topics: ['red', 'green']
      },
      {
        name: 'second',
        timestamp: Date.now() - 3600 * 24 * 30,
        topics: ['blue', 'yellow']
      },
      {
        name: 'third',
        timestamp: Date.now() - 3600 * 24 * 30 * 2,
        topics: ['red', 'blue']
      }
    ], startQueries)
  })

  async function startQueries (go) {
    try {
      await queryAndLog(peer1, 'red', '(on peer1)')
      await queryAndLog(peer2, 'red', '(on peer2 before remote query)')
      // Now peer2 sends a query to its peers for some data.
      peer2.remoteQuery('topics', { topic: 'red' })
      await new Promise(resolve => setImmediate(resolve))
      await queryAndLog(peer2, 'red', '(on peer2 after remote query)')
      console.log('done')
    } catch (err) { console.error(err) }
  }

  function queryAndLog (peer, topic, msg) {
    return new Promise((resolve, reject) => {
      queryTopicsLocally(peer, 'red', (err, result) => {
        if (err) return console.error(err)
        logResult(result, `query "${topic}" ${msg}:`, logFeeds(peer))
        resolve()
      })
    })
  }

  function queryTopicsLocally (peer, topic, cb) {
    peer.kappa.ready(() => {
      collect(
        peer.kappa.view.topics.query({ topic })
          .pipe(peer.indexer.createLoadStream()),
        cb
      )
    })
  }

  function logResult (result, prefix, suffix) {
    let str = result.map(r => (
      `  ${pretty(r.key)}@${r.seq}: "${r.value.name}", (${r.value.topics.join(', ')})`
    )).join('\n')
    console.log(`${prefix}\n${str || '  no results'}`)
    if (suffix) console.log(suffix)
    console.log()
  }

  function logFeeds (peer) {
    return 'Feeds: \n' + Object.values(peer.feeds._feeds).map(feed => {
      return `  Key: ${pretty(feed.key)} Len: ${feed.length} Blocks: ${feed.downloaded()}`
    }).join('\n')
  }
}

function createApp (name) {
  // We need a couple of leveldbs for state and view persistence.
  const dbs = {
    indexer: mem(),
    recent: mem(),
    topics: mem()
  }

  // Let's use a multifeed as our datasource.
  // This example could also look very similar with e.g. corestore.
  const feeds = multifeed(ram, {
    // We start in sparse mode! This means no data is downloaded from our peers by default.
    sparse: true,
    valueEncoding: 'json'
  })

  // And let's set up an indexer. The indexer will watch a list of feeds
  // for download and append events, and keep a global local log of all
  // key@seq pairs ("feed ids").
  const indexer = new Indexer({
    db: dbs.indexer,
    name
  })

  // Let's add all the feeds from the multifeed to our indexer.
  feeds.ready(() => {
    feeds.feeds().forEach(feed => indexer.add(feed))
    feeds.on('feed', feed => indexer.add(feed))
  })

  // Now we create a Kappa core. The Kappa drives our app specific
  // views into the data.
  const kappa = new Kappa()
  // We define two views. Both should be filled with everything
  // from our indexer.
  kappa.use('recent', indexer.source(), createRecentView(dbs.recent))
  kappa.use('topics', indexer.source(), createTopicsView(dbs.topics))

  // Now, because we start in sparse mode, we need to ask our peers for
  // data. We use hypercore-query-extension for this. There, we
  // define two queries that we can both reply for if peers ask us,
  // and ask peers ourselves.
  // On requests, we answer the queries with info from our views.
  const query = new Query({
    api: {
      recent (args) {
        const { from, to, live } = JSON.parse(args.toString())
        const res = kappa.view.recent.query({ from, to, live })
        return res.pipe(keyToBuffer())
      },
      topics (args) {
        const { topic, live } = JSON.parse(args.toString())
        const res = kappa.view.topics.query({ topic, live })
        return res.pipe(keyToBuffer())
      }
    }
  })

  // This is a little helper function that feeds the results
  // from a remote query directly into the indexer.
  // The indexer then downloads the resulting key/seq pairs.
  function remoteQuery (name, args) {
    args = JSON.stringify(args)
    const results = query.query(name, args)
    results.pipe(downloadBlocks(feeds))
  }

  // This is our "app":
  return {
    feeds,
    indexer,
    kappa,
    remoteQuery,

    // This won't be needed once multifeed supports registerExtension
    replicate (isInitiator, opts) {
      const stream = feeds.replicate(isInitiator, opts)
      stream.registerExtension('query', query.extension())
      return stream
    }
  }
}

function replicate (a, b) {
  const sa = a.replicate(true, { live: true })
  const sb = b.replicate(false, { live: true })
  sa.pipe(sb).pipe(sa)
}

function ready (a, b, cb) {
  a.feeds.ready(() => b.feeds.ready(() => cb()))
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
        return db.createReadStream(opts).pipe(keyseqFromKey())
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
        return db.createReadStream(opts).pipe(keyseqFromKey())
      }
    }
  }
}

function downloadBlocks (feeds) {
  return new Writable({
    objectMode: true,
    write (row, enc, next) {
      const feed = feeds.feed(row.key)
      if (feed) feed.download(Number(row.seq))
      next()
    }
  })
}

function keyseqFromKey () {
  return new Transform({
    objectMode: true,
    transform (msg, enc, next) {
      const keyseq = msg.key.split('/')[1]
      const [key, seq] = keyseq.split('@')
      this.push({ key, seq })
      next()
    }
  })
}

function keyToBuffer () {
  return new Transform({
    objectMode: true,
    transform (row, enc, next) {
      const key = Buffer.from(row.key, 'hex')
      const seq = row.seq
      this.push({ key, seq })
      next()
    }
  })
}
