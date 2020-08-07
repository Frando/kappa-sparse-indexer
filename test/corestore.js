const hypercore = require('hypercore')
const { promisify } = require('util')
const ram = require('random-access-memory')
const mem = require('level-mem')
const Indexer = require('..')
const tape = require('tape')
const Corestore = require('corestore')
const Kappa = require('kappa-core')
const createCorestoreSource = require('kappa-core/sources/corestore')
const collect = promisify(require('stream-collector'))
const { createTopicsView } = require('./lib/views')
const { replicate } = require('./lib/util')

function loadValueFromCorestore (corestore) {
  return function loadValue (message, next) {
    const feed = corestore.get({ key: message.key })
    feed.get(message.seq, { wait: false }, (err, value) => {
      if (!err) message.value = value
      next(message)
    })
  }
}

tape('corestore indexed multi view', async t => {
  const store = new Corestore(ram, {
    valueEncoding: 'json'
  })
  const indexer = new Indexer({
    db: mem(),
    loadValue: loadValueFromCorestore(store)
  })
  const kappa = new Kappa()
  kappa.use('index',
    createCorestoreSource({ db: mem(), store }),
    indexer.createView()
  )
  kappa.use('topics',
    indexer.createSource(),
    createTopicsView(mem())
  )
  kappa.on('state-update', (name, state) => {
    console.log('state update', name, state)
  })
  await store.ready()
  const feed = store.get()
  await append(feed, { name: 'alice', topics: ['blue'] })
  await append(feed, { name: 'bob', topics: ['blue', 'red'] })
  const feed2 = store.get()
  await append(feed2, { name: 'claire', topics: ['green', 'red'] })
  console.log('appended', feed)
  console.log('before')
  await ready(kappa, 'index')
  await ready(kappa, 'topics')
  console.log('after')

  setTimeout(() => {
    append(feed2, { name: 'diego', topics: ['pink', 'green'] })
  }, 10)

  const key = feed.key.toString('hex')
  const key2 = feed2.key.toString('hex')

  const reds = await collect(
    kappa.api.topics.query('red').pipe(indexer.createLoadStream())
  )
  t.deepEqual(reds.map(r => r.value.name).sort(), ['bob', 'claire'])

  const blues = await collect(
    kappa.api.topics.query('blue').pipe(indexer.createLoadStream())
  )
  t.deepEqual(blues.map(r => r.value.name).sort(), ['alice', 'bob'])
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
  return new Promise((resolve, reject) => {
    feed.append(value, err => err ? reject(err) : resolve())
  })
}
