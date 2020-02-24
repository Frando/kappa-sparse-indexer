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
    indexer1.sync(() => {
      console.log('ready')
      indexer1.read((err, messages) => {
        t.error(err)
        t.equal(messages.length, 2)
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
    const sub = indexer2.createSubscription()
    feed1.append(['foo', 'bar', 'baz'], () => {
      replicate(feed1, feed2, () => {
        indexer2.sync(() => {
          const stream = sub.createPullStream()
          const values = []
          stream.on('data', node => {
            values.push(node)
            sub.setCursor(node.lseq)
            stream.destroy()
            const stream2 = sub.createPullStream()
            stream2.on('data', node => {
              values.push(node)
            })
            stream2.on('end', () => {
              t.equal(values.length, 3)
              t.deepEqual(values.map(n => n.value.toString()).sort(), ['bar', 'baz', 'foo'])
              t.end()
            })
          })
        })
      })
    })
  })
})
