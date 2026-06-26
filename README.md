# Swarm FS

Swarm FS is an upload layer on top of Bee that stamps chunks client-side and tracks which slots in a postage batch are occupied by each file. This makes it possible to "delete" a file by reclaiming its slots for future uploads — something Swarm has no native concept of.

## Usage

Environment variables can be set in your shell profile for global use, or in a local `.env` file if you prefer per-project configuration:

```sh
export SWARMFS_UPLOAD_URL="http://localhost:1633/chunks"
export SWARMFS_SIGNER="<private key hex>"
export SWARMFS_BATCH_ID="<batch id hex>"
export SWARMFS_BATCH_DEPTH=<depth of your batch>
```

```sh
# Upload a file and print its root hash
swarm-fs upload <file>

# List all tracked files
swarm-fs list

# Delete a file by root hash, reclaiming its slots
swarm-fs delete <root hash>

# Show slot usage and most utilized bucket
swarm-fs status
```

State is stored in `~/.swarmfs/`, with files named by the first 8 hex characters of the batch ID (e.g. `swarmfs-a1b2c3d4.free`, `swarmfs-a1b2c3d4.idx`). Multiple postage batches can be used independently by switching `SWARMFS_BATCH_ID`.

## Design

Swarm FS stamps chunks client-side, which means it controls which `(bucket, slot)` each chunk is assigned to before sending the pre-signed chunk to the Bee API. This makes it possible to maintain a local index that tracks exactly which slots are occupied by each file, and to deliberately target previously freed slots when uploading new files.

All state lives under a `.swarmfs/` directory.

### `swarmfs.free` — SlotMap

A bitmap tracking slot occupancy across the entire batch. Each bit represents one slot: 0 = free, 1 = occupied. Pre-allocated at init time as all zeros, so the full batch capacity is available from the start.

```
per bucket (65536 entries, indexed by bucket number):
  [slot bitmap: slotsPerBucket / 8 bytes]
```

The fixed-size layout allows O(1) access by bucket index. Allocating a slot scans the bucket's bitmap for the first 0 bit and sets it to 1. Freeing a slot clears the corresponding bit. File size is determined by batch depth: for depth 24 (256 slots per bucket), the file is 2 MB.

### `swarmfs.idx` — FileRegistry

Tracks all uploaded files, their root chunk hash, and the list of slots they occupy.

```
[file count: uint32]

per file:
  [path length: uint16]
  [path: utf8 bytes]
  [root hash: 32 bytes]
  [chunk count: uint32]
  [chunk 0: bucket uint16, slot uint16]
  [chunk 1: bucket uint16, slot uint16]
  ...
```

Overhead is approximately 4 bytes per chunk — about 0.1% of the file size — regardless of file size.

### Upload flow

1. Split the file into chunks client-side
2. For each chunk, allocate a slot from `swarmfs.free` for the matching bucket
3. Sign the stamp with the chosen `(bucket, slot)` and send the pre-signed chunk to Bee
4. Record the root hash and all `(bucket, slot)` pairs in `swarmfs.idx`

### Deletion flow

1. Look up the file's chunk list in `swarmfs.idx`
2. Return all its `(bucket, slot)` pairs to `swarmfs.free` under their respective buckets
3. Remove the file entry from `swarmfs.idx`
4. Optionally upload tombstone chunks to overwrite the slots on the network
