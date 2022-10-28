const Hypercore = require('hypercore')
const ram = require('random-access-memory')
const mem = require('level-mem')
const Indexer = require('..')
const tape = require('tape')
const { replicate } = require('./lib/util')
const { collectStream } = require('../lib/util')

tape('scan', async t => {
  const feed1 = new Hypercore(ram)
  const indexer1 = new Indexer(mem())
  await feed1.append(['foo', 'bar'])
  indexer1.add(feed1, { scan: true })
  await indexer1.sync()
  const messages = await indexer1.read()
  t.equal(messages.length, 2)
})

tape('replicate', async t => {
  const feed1 = new Hypercore(ram)
  await feed1.ready()
  const indexer1 = new Indexer(mem())
  indexer1.add(feed1)
  const sub = indexer1.createSubscription()
  const rows = ['foo', 'bar', 'baz']
  await feed1.append(rows)
  await indexer1.sync()
  const stream1 = sub.createPullStream()
  const res1 = await collectStream(stream1)
  t.deepEqual(rows, res1.map(r => r.value.toString()))

  const feed2 = new Hypercore(ram, feed1.key)
  const indexer2 = new Indexer(mem())
  indexer2.add(feed2)

  replicate(feed1, feed2)

  await feed2.update()
  await indexer2.sync()
  const stream2 = sub.createPullStream()
  const res2 = await collectStream(stream2)
  t.deepEqual(rows, res2.map(r => r.value.toString()))
})

tape('live', async t => {
  const feed1 = new Hypercore(ram)
  const indexer1 = new Indexer(mem())
  indexer1.add(feed1)
  const sub = indexer1.createSubscription()
  const stream1 = sub.createPullStream({ live: true })
  await feed1.append('foo')

  setTimeout(() => {
    feed1.append('bar')
  }, 100)

  let i = 0
  const values = []
  for await (const row of stream1) {
    i += 1
    values.push(row.value.toString())
    if (i === 2) break
  }
  t.deepEqual(values, ['foo', 'bar'])
})

tape('live replication', async t => {
  const feed1 = new Hypercore(ram)
  const indexer1 = new Indexer(mem())
  indexer1.add(feed1)
  const sub = indexer1.createSubscription()
  const stream1 = sub.createPullStream({ live: true })
  await feed1.append('foo')
  const feed2 = new Hypercore(ram)
  await feed2.ready()
  const feed3 = new Hypercore(ram, feed2.key)
  replicate(feed2, feed3)
  feed3.download()
  indexer1.add(feed3)

  setTimeout(() => {
    feed1.append('bar')
    feed2.append('boo')
  }, 100)

  let i = 0
  const values = []
  for await (const row of stream1) {
    i += 1
    values.push(row.value.toString())
    if (i === 3) break
  }
  t.deepEqual(values, ['foo', 'bar', 'boo'])
})
