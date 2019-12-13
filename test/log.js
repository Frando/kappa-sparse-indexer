const Materialized = require('../lib/log')
const mem = require('level-mem')
const tape = require('tape')

tape('log', t => {
  const log = new Materialized(mem())
  log.append('A', 1)
  log.append('B', 5)
  log.append('C', 2)
  log.append('C', 1)
  log.append('B', 4)
  log.flush(() => {
    log.read(2, 4, (err, res) => {
      t.error(err)
      res = res.map(r => r.key + r.seq).join(' ')
      t.equal(res, 'B5 C2 C1')
      log.keyheads((err, heads) => {
        t.error(err)
        t.deepEqual(heads, { A: 1, B: 5, C: 2 })
        t.end()
      })
    })
  })
})
