# kappa-sparse-indexer

An indexer for hypercores with support for sparse indexing and exposing the indexed data to Kappa views.

Based on [an in-progress kappa-core version](https://github.com/kappa-db/kappa-core/pull/14).

See [example.js](example.js) for a full example on how this can be used with [hypercore-query-extension](https://github.com/peermaps/hypercore-query-extension/) and [multifeed](https://github.com/kappa-db/multifeed) to do efficient sparse syncing of collections of hypercores.

More docs to come soon! For now check out the example (will also be expanded soon)

## API

`const Indexer = require('kappa-sparse-indexer')`

#### `const indexer = new Indexer(leveldb, opts)`

Create a new indexer. `leveldb` must be a [level](https://github.com/Level/level) instance (or compatible). `opts` are:

* `name: string` A name (for debugging purposes only)
* `loadValue: function` A callback to load a value from a `(key, seq)` pair

#### `indexer.add(feed, opts)

Add a feed to the indexer. Opts are:

* `scan: false` Set to true to scan for undownloaded blocks initially.

### `indexer.createReadStream({ start: 0, end: Infinity, limit })

Create a read stream on the combined log. Messages emitted look like this:
```
{
  key: "hex-encoded key of a feed",
  seq: Number, // The seq of this message in its feed
  lseq: Number, // The "local seq" of this message in the materialized log
  value: object // The value if opts.loadValue isn't false
}
```

### `indexer.read({ start, end, limit }, cb)`

Similar to `createReadStream` but collect messages and calls cb with `(err, result)`, where `result` is:
```
{
  messages, // Array of messages
  cursor: Number, // The last lseq of the batch of messages,
  finished: bool, // true if there are no more messages to read after this batch
}
```

### `indexer.source()`

Create a source for a kappa-core@experimental

### `indexer.createSubscription()`

Create a stateful subscription (where each read call returns the same as above plus an `ack` function that when called advances the cursor so that the next read call returns the next batch of messages).


