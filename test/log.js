const Log = require('../lib/log')
const mem = require('level-mem')
const tape = require('tape')
const collect = require('stream-collector')

tape('log', t => {
  const db = mem()
  const log = new Log(db)
  log.append('A', 1)
  log.append('B', 5)
  log.append('C', 2)
  log.append('C', 1)
  log.append('B', 4)
  log.flush(() => {
    check(log, 'first', () => {
      const log2 = new Log(db)
      // collect(db.createReadStream(), (err, rows) => {
      //   console.log(err, rows)
      //   t.end()
      // })
      log2.open(() => {
        check(log2, 'reopen', () => {
          // const rs = db.createReadStream()
          // rs.on('data', console.log)
          // rs.on('end', () => t.end())
          t.end()
        })
      })
    })
  })

  function check (log, msg, cb) {
    log.read({ start: 2, end: 4 }, (err, res) => {
      t.error(err, msg + ' no err')
      res = res.map(r => r.key + r.seq).join(' ')
      t.equal(res, 'B5 C2 C1', msg + ' read ok')
      t.equal(log.length, 5)
      log.keyheads((err, heads) => {
        t.error(err)
        t.deepEqual(heads, { A: 1, B: 5, C: 2 }, msg + ' keyheads ok')
        cb()
      })
    })
  }
})

tape('log many', t => {
  const db = mem()
  const log = new Log(db)
  const COUNT = 5000
  for (let i = 0; i < COUNT; i++) {
    log.append('X', i)
  }
  log.flush(() => {
    let pending = COUNT

    for (let i = 0; i < COUNT; i++) {
      log.lookup({ key: 'X', seq: i }, (err, res) => {
        if (err) t.fail(err)
        if (res !== i + 1) t.fail(`Results don't match: ${res} !== ${i + 1}`)
        if (--pending === 0) {
          t.pass('OK')
          t.end()
        }
      })
    }
  })
})
