const hypercore = require('hypercore')
const ram = require('random-access-memory')
const mem = require('level-mem')
const Indexer = require('..')
const tape = require('tape')
const { replicate } = require('./lib/util')

tape('scan', t => {
  const feed1 = hypercore(ram)
  const indexer1 = new Indexer(mem())
  feed1.append(['foo', 'bar'], () => {
    console.log('appended')
    indexer1.add(feed1, { scan: true })
    indexer1.ready(() => {
      console.log('ready')
      indexer1.read((res) => {
        t.equal(res.messages.length, 2)
        // console.log(res)
        t.end()
      })
    })
  })
})

tape('replicate', t => {
  const feed1 = hypercore(ram)
  feed1.ready(() => {
    const feed2 = hypercore(ram, feed1.key)
    const indexer2 = new Indexer(mem())
    indexer2.add(feed2)
    feed1.append(['foo', 'bar'], () => {
      replicate(feed1, feed2, () => {
        indexer2.ready(() => {
          indexer2.read(res => {
            t.equal(res.messages.length, 2)
            t.end()
          })
        })
      })
    })
  })
})
