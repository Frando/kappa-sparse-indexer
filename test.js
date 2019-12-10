const Materialized = require('./lib/log')
const mem = require('level-mem')
const tape = require('tape')

tape('log', t => {
  const log = new Materialized(mem())
  log.append('alpha', 1)
  log.append('beta', 5)
  log.append('gamma', 2)
  log.append('gamma', 1)
  log.append('beta', 4)
  log.flush(() => {
    log.read(2, 4, (err, res) => {
      t.error(err)
      console.log('log 2-4', res)
      log.heads((err, heads) => {
        t.error(err)
        console.log('heads', heads)
        t.end()
      })
    })
  })
})
