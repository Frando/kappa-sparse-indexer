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

#### `indexer.add(feed, opts)

Add a feed to the indexer. Opts are:

* `scan: false` Set to true to scan for undownloaded blocks initially.
