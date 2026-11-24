# SlateDB: A 10K Overview

## 1. What SlateDB is

SlateDB is an embedded key-value storage engine. It uses a log-structured merge-tree
(LSM tree) design. The key difference from RocksDB, LevelDB, and Pebble is simple:
SlateDB writes all persistent data to object storage. The object store is the only
source of truth. SlateDB is a library, not a server.

The repository is version 0.18.1, in Rust, under Apache 2.0. The project is a member of
the Commonhaus Foundation.

## 2. Why object storage

Traditional LSM engines assume local disks. Object storage gives bottomless capacity,
high durability, and low operational burden, but it charges per API request and has
higher latency. SlateDB accepts that trade and uses a zero-disk architecture, batching,
read caches, Bloom filters, compression, writer fencing, and multi-reader support.

It suits workloads that tolerate 50-100ms write latency or pay for frequent PUTs:
stream processing, serverless functions, durable execution, and durable caches.

## 3. Non-goals

The project avoids a server or network protocol, columnar storage, multiple writers per
database, and maximum performance at any cost. CLEAN_SLATE.md documents these choices;
the project prefers incremental changes and copies proven ideas from RocksDB, LevelDB,
Badger, and Pebble.

## 4. Core architecture

The main parts are the **Db** client, the **WAL** (an append-only log stored as SSTables
in `wal/`), the **MemTable** (a mutable sorted in-memory map), **L0 SSTs** (immutable
tables flushed from MemTables; each covers the whole key space), **sorted runs** (range-
partitioned tables produced by compaction), and the **manifest** (a FlatBuffer that
records L0 SSTs, sorted runs, the WAL start point, checkpoints, and the writer epoch).

The compactor merges tables, the garbage collector deletes unreferenced files,
`DbReader` serves read-only clients, and `Admin` handles manifests, checkpoints,
clones, GC, and compaction.

## 5. Object-store layout

```
path/to/db/
  manifest/      <id>.manifest      complete state snapshots, FlatBuffers
  wal/           <id>.sst           write-ahead log tables
  compactions/   <id>.compactions   compactor state snapshots
  compacted/     <ulid>.sst         L0 and sorted-run tables
  gc/            *.boundary         GC high-watermark files
```

Manifest and WAL IDs are 20-digit integers; compacted SSTs use ULIDs; the schema is in
`schemas/manifest.fbs`. Processes poll the manifest every second by default to find new
WAL data, compaction output, and fencing changes. Writers replace the manifest with
compare-and-swap (CAS), keeping metadata consistent on object stores with atomic replace
only.

## 6. Write path

A `put()`, `delete()`, `merge()`, or `write()` call follows these steps:

1. The client builds a write batch and sends it to a single writer event loop.
2. The loop assigns a sequence number from an oracle that starts at 1.
3. The batch appends to the in-memory WAL buffer and inserts into the MemTable.
4. The call returns a `WriteHandle`, before object storage is touched.

Background tasks handle durability. The WAL flusher uploads a WAL SST when the buffer is
full or 100ms passes. The MemTable flusher uploads an L0 SST at `l0_sst_size_bytes`,
default 64 MiB. When uploads finish, the durable sequence number advances.

Call `handle.await_durable().await` to wait for one write, or `db.flush().await` to
flush all pending writes. Backpressure applies when unflushed bytes exceed
`max_unflushed_bytes`, default 1 GiB.

A 100ms `flush_interval` costs about $130 per month in PUT charges on S3 Standard, per
the docs. Lower intervals reduce latency and raise cost. Callers can supply sequence
numbers through `WriteOptions::seqnum`; the value must exceed the current maximum.

## 7. Read path

A read starts at a snapshot sequence number; the latest state is the default, and
`DbSnapshot` pins a point in time. The engine checks the mutable and immutable
MemTables, then builds iterators over all L0 SSTs and sorted runs in parallel. It uses
each SST block index to find the candidate block, caches fetched blocks, and merges the
iterators into one view that respects sequence numbers and the snapshot.

WAL SSTs are read only during recovery and CDC. Read-only clients use `DbReader` in
managed-checkpoint, fixed-checkpoint, or follow-latest mode.

## 8. Consistency, durability, and fencing

SlateDB provides snapshot isolation. A snapshot pins the visible sequence number without
blocking writers; later writes are invisible to it.

A write is visible in memory when it enters the MemTable. It is durable when its WAL SST
uploads, or when its MemTable flushes if the WAL is disabled. On restart, SlateDB
replays the WAL from the manifest's recorded start point.

Single-writer enforcement uses the manifest `writer_epoch`. A new writer increments the
epoch with a CAS update. A writer that cannot refresh its epoch is fenced; later
operations fail with a `Fenced` error. Zombie writers become harmless.

## 9. Compaction

Compaction reduces read and space amplification by merging L0 SSTs and sorted runs. The
compactor has an event loop, event handler, scheduler, and executor.

The default scheduler is size-tiered, and the default executor is
`TokioCompactionExecutor`, which runs locally. `CompactionWorker` runs compaction on a
separate process or machine. Scheduler and executor traits make policies pluggable.

Trivial moves reuse non-overlapping input SSTs without rewriting data; they are off by
default. Incremental compaction publishes partial output for long jobs and is
experimental. Filters can drop or rewrite rows. Segmented compaction splits keys into
independent LSM trees through a `PrefixExtractor`, so each segment gets its own policy.
Manifest union and projection support split/merge and clones. State persists in
`compactions/`, so a restart resumes work.

## 10. Garbage collection

The garbage collector runs in the background inside a client process. It checks WAL
SSTs, fence SSTs, compacted SSTs, manifests, and compaction state independently. Each
type has an interval and a minimum age; set it to `None` to disable, or use `dry_run`.

GC deletes a file only when no active manifest or checkpoint references it and it is
older than `min_age`. A `GcFilter` can approve or reject candidates, for example to
confirm mirroring.

Boundary files in `gc/` stop stale writers from reviving deleted metadata. GC advances
a boundary conditionally, and a writer behind it fails.

Checkpoints are durable references to one manifest. They add metadata only, keep old
manifests and SSTs alive for long reads and clones, and are cheap to create.

## 11. Feature set

The API includes point operations (`get`, `put`, `delete`, `merge`), batch writes with
TTL and sequence-number options, and scans (`scan`, `scan_prefix`,
`scan_prefix_by_recency`, `seek`, reverse order). It also provides snapshots,
transactions, checkpoints, clones, CDC, reader handles, admin operations, metrics, and
tracing.

Transactions use optimistic concurrency. `IsolationLevel::Snapshot` detects write-write
conflicts; `SerializableSnapshot` also detects read-write conflicts. Checks use sequence
numbers, not extra I/O.

Clones are shallow: the clone manifest points at source SSTs and records which external
checkpoints keep them alive. Nested clones work the same way. CDC uses `SlateDbWalReader`
to stream WAL rows with a durable cursor; it is a live feed, not a backfill API.

## 12. Caching and performance

Two cache layers serve SST reads. The **block cache** stores decoded blocks, indexes,
filters, and stats. The default `SplitCache` separates data blocks from metadata;
`FoyerHybridCache` adds local disk. The **object-store cache** stores raw bytes; the old
`CachedObjectStore` is deprecated in favor of `ObjectStoreMirror` and the Foyer cache.

Point reads cache data blocks by default; scans do not, so long sequential reads do not
evict hot data. Both always cache SST metadata. `BlockCachePolicy` controls what flushes
and compactions insert.

SSTs use Bloom filters at `min_filter_keys` keys, default 1,000. Compression is
optional (Snappy, Zlib, LZ4, Zstd). SlateDB works with any `object_store`
implementation and re-exports the crate.

## 13. Operations and tuning

`Settings` holds configuration. Defaults favor low latency: 100ms flush interval, 1s
manifest poll, 64 MiB L0 tables, 1 GiB of unflushed write backpressure, and four
parallel flushes. Compaction and GC are on; compression is off.

`DbBuilder` configures segments, caches, merge operators, and block cache policy.
`DbReaderBuilder` handles read-only clients; `CompactorBuilder` and
`GarbageCollectorBuilder` run services separately.

The `slatedb` CLI manages databases. `slatedb-bencher` runs YCSB-style and db_bench-style
workloads; results are at benchmark.slatedb.io. `slatedb-dst` provides deterministic
simulation tests with a seeded runtime, mock clock, failpoints, and fault-injecting
stores.

## 14. Bindings and ecosystem

Official bindings use UniFFI and ship for Go, Java, Node.js, and Python. Community
bindings exist for .NET, Ruby, and TypeScript. Adopters include Dropbox, Prisma, Gadget,
Goldsky, HelixDB, and Tensorlake.

SlateDB releases every two months, at the end of each even month. Storage formats are
compatible between adjacent versions. The API is not stable; compile-time breaks are
allowed. Version 0.18.1 is pre-1.0, so evaluate carefully before production use.

## 15. When to use it

Choose SlateDB for an embedded KV store with no servers, no local disks, and object
storage as the durable layer. It suits batched workloads that need snapshots,
transactions, clones, or CDC without a database service.

Avoid it for sub-10ms writes, multi-writer databases, strict API stability, or columnar
analytics, or when PUT cost or request latency dominates the budget.

In short: SlateDB is RocksDB-shaped storage on S3-shaped infrastructure. Its promise is
cloud-native durability and capacity; its price is write latency and API cost.

## Deep dive storage format
## Short answer

The MemTable has no byte encoding. It is an in-memory skip map of Rust structs, and it
shares `Bytes` allocations with the write batch and WAL buffer.

The SST is the byte format. It stores rows in 4 KiB blocks. The current V2 row codec uses
prefix compression, LEB128 lengths, a flags byte, and optional timestamps. SlateDB then
compresses each block and adds a CRC32. It also truncates index keys, writes Bloom
filters only for large SSTs, and keeps the footer to 10 bytes.

## 1. MemTable data structure

### 1.1 The map

`slatedb/src/mem_table.rs` defines the MemTable as:

```rust
SkipMap<SequencedKey, RowEntry>

SequencedKey { user_key: Bytes, seq: u64 }

RowEntry {
    key: Bytes,
    value: ValueDeletable,
    seq: u64,
    create_ts: Option<i64>,
    expire_ts: Option<i64>,
}

enum ValueDeletable {
    Value(Bytes),
    Merge(Bytes),
    Tombstone,
}
```

The map is a `crossbeam_skiplist::SkipMap`. It is lock-free and supports concurrent
readers and writers. `Ord for SequencedKey` sorts by user key ascending, then by sequence
number descending. The newest version of a key comes first. A range query maps a user key
range to `(key, u64::MAX) ..= (key, 0)` so that it includes every version.

### 1.2 There is no memory encoding

The MemTable stores live Rust values. It does not serialize keys or values. Three
mechanisms avoid copies:

* `Bytes` is reference-counted. The map key and the `RowEntry` both hold `Bytes` handles
  to the same key allocation. The key bytes are not duplicated.
* `WriteBatch::put_bytes` accepts owned `Bytes`, so a caller with a buffer pool can avoid
  the copy that `put` performs.
* `WriteBatch` stores operations in a `BTreeMap<Bytes, SmallVec<[WriteOp; 1]>>`. A later
  put or delete for the same key replaces earlier puts and deletes in that batch. Merge
  operands are kept in order. Only the resulting rows reach the MemTable.

The WAL buffer also clones `RowEntry` values. Those clones copy only the `Bytes` handles.
The write batch, WAL buffer, and MemTable share the underlying key and value buffers until
the WAL or an SST encodes them.

### 1.3 Size accounting is logical, not physical

`RowEntry::estimated_size()` returns:

```text
key.len() + value.len() + 8 (seq) + 8 if create_ts + 8 if expire_ts
```

`KVTable` keeps this total in `entries_size_in_bytes`. It is updated on overwrite. The
MemTable freezes at `l0_sst_size_bytes`, default 64 MiB.

This number excludes:

* The `Bytes` header for each handle.
* Skiplist tower pointers, nodes, and allocator overhead.
* The second `Bytes` handle in `SequencedKey`.
* Enum tags and padding.

So 64 MiB of logical data uses more than 64 MiB of RSS, especially for small keys and
values. SlateDB does not compress the MemTable. Compression starts at the WAL/SST boundary.

Multiple versions and tombstones stay in memory until a flush and later compaction remove
them. TTL does not free memory immediately. Expired rows are skipped on read, then
rewritten or dropped during compaction.

## 2. SST file layout

The file is built in this order (`slatedb/src/format/sst.rs`, `sst_builder.rs`):

```text
+---------------------------+
| Data blocks               |  each: encoded entries, then compressed/transformed, CRC32
+---------------------------+
| Filter block (optional)   |  composite named filters, compressed, CRC32
+---------------------------+
| Index block               |  FlatBuffer SsTableIndex, compressed, CRC32
+---------------------------+
| Stats block (optional)    |  FlatBuffer SstStats, compressed, CRC32
+---------------------------+
| SsTableInfo               |  FlatBuffer metadata, CRC32
+---------------------------+
| Metadata offset: u64      |  8 bytes
| Format version: u16       |  2 bytes
+---------------------------+
```

Each data block is a self-contained block. The SST index stores one `BlockMeta` per data
block. That record holds the block offset, a separator key, and an optional encoded
length.

### 2.1 V2 row encoding

V2 is the default (`SST_FORMAT_VERSION_LATEST = 2`). A row is
(`slatedb/src/format/row_codec_v2.rs`):

```text
shared_bytes   LEB128 u32    bytes shared with the previous key
unshared_bytes LEB128 u32    length of the key suffix
value_len      LEB128 u32    value length; 0 for tombstones
key_suffix     bytes
value          bytes         omitted for tombstones
seq            u64           fixed width, big-endian
flags          u8            bit field
expire_ts      i64           only if HAS_EXPIRE_TS
create_ts      i64           only if HAS_CREATE_TS
```

Flags are (`slatedb/src/format/row.rs`):

| Bit | Meaning |
|-----|---------|
| `0x01` | Tombstone |
| `0x02` | Has `expire_ts` |
| `0x04` | Has `create_ts` |
| `0x08` | Merge operand |

The three lengths use LEB128, so a small length costs one byte. Sequence numbers and
timestamps stay fixed-width. The code comments explain why: sequence numbers are large,
timestamps rarely compress well, and fixed-width parsing is simpler and faster.

Endianness is mixed by design. The row fields use `bytes::BufMut::put_u64` and `put_u16`,
which write big-endian. FlatBuffers inside the index, stats, and `SsTableInfo` use
FlatBuffers' little-endian layout.

> The SST design page labels the footer integers "LE". The code uses `put_u64` and
> `put_u16`, which are big-endian. The source is authoritative.

### 2.2 V1 legacy encoding

SST format version 1 uses the older `SstRowCodecV0` (`slatedb/src/format/row.rs`):

```text
key_prefix_len u16
key_suffix_len u16
key_suffix
seq            u64
flags          u8
expire_ts      i64 (optional)
create_ts      i64 (optional)
value_len      u32 (omitted for tombstones)
value          bytes
```

V1 measures the prefix against the block's first key. V2 measures it against the previous
key. V2 therefore adapts better to sorted keys whose common prefix changes across the
block. Readers still support V1.

### 2.3 Block trailer and prefix compression

Both block formats share the `Block` struct. Its `offsets` field means different things:

* V1: a `u16` offset for every entry, followed by the entry count as `u16`.
* V2: one `u16` restart offset every 16 entries, followed by the restart count as `u16`.

V2's restart points are the key trade. Every 16th row stores the full key with
`shared_bytes = 0`. The reader binary-searches the restart offsets, decodes that full key,
then scans forward and rebuilds keys from the previous key. This removes a 2-byte
per-entry offset. It also makes seeks possible without decoding from block start.

Prefix compression applies only inside a block. A new block restarts compression, because
the first key has no previous key. A larger block therefore compresses better and reduces
index size. A smaller block gives finer read granularity.

### 2.4 Index block

The index is a FlatBuffer `SsTableIndex` containing `BlockMeta` records
(`schemas/sst.fbs`):

```text
BlockMeta {
    offset: ulong;
    first_key: [ubyte] (required);
    encoded_len: uint;   // 0 when the block is not padded
}
```

The index holds one entry per data block. For compacted SSTs:

* The first block's key is empty.
* Later entries store `compute_lower_bound(previous_block_last_key, this_block_first_key)`.
  This is a separator key, often a shortened prefix. It is not necessarily a real key.
* With no block alignment, `encoded_len` is zero and the reader derives the block length
  from the next block offset. That saves 4 bytes per block.

For WAL SSTs, the index `first_key` holds the block's first sequence number as 8 bytes.
There is no filter and no stats block. WAL rows are in sequence order, so prefix
compression still works but usually saves less than in key-sorted SSTs.

### 2.5 Filter block

A compacted SST gets a Bloom filter only when `num_rows >= min_filter_keys`, default
1,000. WAL SSTs never get one.

The filter block is composite:

```text
[num_filters u16]
for each filter:
    [name_len u16][name bytes][data_len u64][data bytes]
```

The built-in Bloom filter payload is:

```text
[num_probes u16][bit array]
```

Default policy is 10 bits per key, whole-key filtering, SipHash13, and enhanced double
hashing. It costs about 1.25 bytes per key, plus the small header. This is extra space,
not compression. It saves object-store reads by skipping SSTs that cannot contain a key.
An optional prefix extractor adds prefix hashes and deduplicates adjacent identical
prefixes, which is why keys must arrive sorted.

### 2.6 Stats and footer

The stats block is a FlatBuffer with total puts/deletes/merges, raw key and value bytes,
and per-block counters. Compacted SSTs write it. It costs space, but it supports read
planning and observability.

`SsTableInfo` is also a FlatBuffer. It stores:

* First and last key, or first sequence number for WAL SSTs.
* Offsets and lengths for the index, filter, and stats blocks.
* Compression codec, SST type, and filter format.

The file ends with an 8-byte metadata offset and a 2-byte format version. Every block,
including `SsTableInfo`, ends with a 4-byte CRC32. The 10-byte footer itself is not
compressed.

## 3. Space-minimization techniques

| Technique | Where | Saves | Cost |
|-----------|-------|-------|------|
| Prefix compression | V2 row codec, previous key | Repeated key bytes inside a block | Key reconstruction cost |
| Restart points every 16 rows | V2 block trailer | 2-byte per-entry offset replaced by 2 bytes per 16 rows | Linear scan from restart |
| LEB128 lengths | V2 row codec | Up to 5 bytes per row vs fixed `u16`+`u16`+`u32` | Variable-length decode |
| Flags byte | Every row | Separate type and null markers | Bit tests |
| Omit value length and bytes for tombstones | Every row | Entire value field for deletes | Branch on flag |
| Optional timestamps | Every row | 8 bytes each when absent | Conditional parse |
| Fixed-width `seq` | Every row | Faster, predictable parse; not a space saving | 8 bytes always |
| Per-block compression | Data, filter, index, stats, info | General redundancy | CPU; can expand incompressible data |
| Truncated separator keys | Index | Shorter index keys | None for correctness |
| One index entry per block | Index | Replaces per-row offsets | Block-level seek |
| Zero `encoded_len` when unpadded | Index | 4 bytes per block | Derive length from next offset |
| Bloom filter threshold | Filter | No filter on small SSTs | Above threshold, ~10 bits/key |
| No filter or stats in WAL | WAL SSTs | Smaller WAL files | No membership pruning on replay |
| Compact 10-byte footer | File end | Fixed small overhead | None |
| Batch overwrite collapse | Write path | Fewer MemTable rows | Bounded work per batch |
| Compaction retention | Compaction | Old versions, expired values, tombstones | Compaction I/O |

Compaction is the largest space reclamation mechanism. `RetentionIterator` always keeps
the newest version, keeps versions needed by active snapshots, converts expired values to
tombstones, drops expired merge operands, and removes trailing tombstones when safe. GC
then deletes SSTs and manifests that no checkpoint or manifest references.

## 4. Practical tuning consequences

* **Block size**: default 4 KiB (`SstBlockSize::Block4Kib`). Larger blocks compress better
  and shrink the index but increase read amplification and cache churn. Smaller blocks do
  the reverse.
* **Compression**: off by default. LZ4 and Zstd are the usual choices for space; Snappy
  favors speed. Compression is per block, so a read decompresses one block, not one file.
* **Bloom filters**: 10 bits per key, enabled after 1,000 rows by default. They cost space
  but avoid reads. Lower `min_filter_keys` gives more pruning at a higher space cost.
* **Block alignment**: pads every block to `block_size` and is incompatible with
  compression. It helps aligned reads but wastes space. It is off by default.
* **MemTable size**: `l0_sst_size_bytes` controls the freeze point, not RSS. Use the
  logical estimate as a lower bound on memory use.
* **WAL size**: WAL SSTs use the same V2 row and block encoding, so write compression also
  reduces WAL bytes and recovery traffic. They omit filters and stats.

**Key sources:** `slatedb/src/mem_table.rs`, `types.rs`, `batch.rs`,
`format/row.rs`, `format/row_codec_v2.rs`, `format/block.rs`, `format/block_v2.rs`,
`format/sst.rs`, `sst_builder.rs`, `filter.rs`, `filter_policy.rs`, `sst_stats.rs`,
`retention_iterator.rs`, `schemas/sst.fbs`, and
`website/src/content/docs/docs/design/sorted-string-table.mdx`.