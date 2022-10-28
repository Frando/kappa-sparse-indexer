const hypercore = require('hypercore')
const { promisify } = require('util')
const ram = require('random-access-memory')
const mem = require('level-mem')
const Indexer = require('..')
const tape = require('tape')
const Corestore = require('corestore')
const Kappa = require('@frando/kappa-core')
const createCorestoreSource = require('@frando/kappa-core/sources/corestore')
const collect = promisify(require('stream-collector'))
const { createTopicsView } = require('./lib/views')
const { replicate } = require('./lib/util')

function loadValueFromCorestore (corestore) {
  return function loadValue (message, next) {
    const feed = corestore.get({ key: Buffer.from(message.key, 'hex') })
    feed.get(message.seq, { wait: false })
      .then((value) => {
        message.value = JSON.parse(value)
        next(message)
      }).catch((err) => {
        next(message)
      })
  }
}

// TODO: Corestore source is not working with corestore v6
tape('corestore indexed multi view', async t => {
  const store = new Corestore(ram)
  const indexer = new Indexer({
    db: mem(),
    loadValue: loadValueFromCorestore(store)
  })
  const kappa = new Kappa()
  kappa.use('index',
    indexer.createSource(),
    indexer.createInputView()
  )
  kappa.use('topics',
    indexer.createSource(),
    createTopicsView(mem())
  )
  kappa.on('state-update', (name, state) => {
    // console.log('state update', name, state)
  })
  await store.ready()
  const feed = store.get({ name: 'feed1' })
  indexer.add(feed)
  await append(feed, { name: 'alice', topics: ['blue'] })
  await append(feed, { name: 'bob', topics: ['blue', 'red'] })
  const feed2 = store.get({ name: 'feed2' })
  await append(feed2, { name: 'claire', topics: ['green', 'red'] })
  indexer.add(feed2, { scan: true })
  await ready(kappa, 'index')
  await ready(kappa, 'topics')

  const reds = await collect(
    kappa.api.topics.query('red').pipe(indexer.createLoadStream())
  )
  t.deepEqual(reds.map(r => r.value.name).sort(), ['bob', 'claire'])

  const blues = await collect(
    kappa.api.topics.query('blue').pipe(indexer.createLoadStream())
  )
  t.deepEqual(blues.map(r => r.value.name).sort(), ['alice', 'bob'])
  await append(feed2, { name: 'diego', topics: ['red', 'green'] })
  await ready(kappa, 'topics')
  const greens = await collect(
    kappa.api.topics.query('green').pipe(indexer.createLoadStream())
  )
  t.deepEqual(greens.map(r => r.value.name).sort(), ['claire', 'diego'])
})

function ready (kappa, ...args) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      // console.log(kappa)
      kappa.ready(...args, err => err ? reject(err) : resolve())
    }, 0)
  })
}

function append (feed, value) {
  return feed.append(JSON.stringify(value))
}
