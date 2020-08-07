# kappa-sparse-indexer

An indexer for hypercores with support for sparse indexing and exposing the indexed data to Kappa views. The indexer listens on `download` and `append` events for all feeds in a set of feeds. It then builds a local materialized log of all downloaded or appended messages. Thus, the indexer provides local total ordering for a set of sparsely synced hypercores. The local ordering gives each message from all feeds a unique, sequential "local sequence number" or `lseq`. This greatly simplifies state handling for subscriptions or derived views, as their state consists of a single integer (their cursor into the local materialized log). Otherwise, to track indexing state for sparsely synced feeds, each view would have to maintain a bitfield for each feed and compare those to the feed's bitfield.

Works great with [an in-progress kappa-core version](https://github.com/kappa-db/kappa-core/pull/14).

See [example.js](example.js) for a full example on how this can be used with [hypercore-query-extension](https://github.com/peermaps/hypercore-query-extension/) and [multifeed](https://github.com/kappa-db/multifeed) to do efficient sparse syncing of collections of hypercores.

## API

`const Indexer = require('kappa-sparse-indexer')`

#### `const indexer = new Indexer(leveldb, opts)`

Create a new indexer. `leveldb` must be a [level](https://github.com/Level/level) instance (or compatible). `opts` are:

* `name: string` A name (for debugging purposes only)
* `loadValue: function (message, next)` A callback to load a value from message object `{ key, seq, lseq }`. Call `next` with the updated message object. If unset and if a feed `key` was added to the indexer, will get the block from that feed and add as `value` to the message object. If set to false value loading will be skipped.

#### `indexer.add(feed, opts)`

Add a feed to the indexer. Opts are:

* `scan: false` Set to true to scan for undownloaded blocks initially. This is required if you cannot ensure that the feed has always been added to the indexer before appending or replicating.

*TODO: An `onwrite` hook set in feed construction would be the safest way to not ever have to use `scan`. When not ever using `scan`, the log's `deduplicate` opt could be set to false, improving performance.

#### `indexer.createReadStream({ start: 0, end: Infinity, limit, live: false })`

Create a read stream on the local materialized log. Messages emitted look like this:
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

### `indexer.createSubscription()`

Create a stateful subscription (where each read call returns the same as above plus an `ack` function that when called advances the cursor so that the next read call returns the next batch of messages).


### `indexer.source()`

Create a source for a [kappa-core@experimental](https://github.com/Frando/kappa-core/tree/experimental). Similar to `createSubscription` but with a little boilerplate so that it can be passed directly into `kappa.use`

## Subscriptions

`indexer.createSubscription` returns a stateful subscription onto the local log. Stateful means that the subscription can track its cursor, making it easy to receive all messages only once. The cursor is not advanced automatically. When calling `pull`, the next set of messages is returned together with an `ack` callback that advances the cursor to the the `lseq` of the last message of the current message set. Thus, the next `read` call would start reading from there. When using `createPullStream`, users have to advance the cursor themselves after each message by calling `subscription.setCursor(message.lseq)`

