---
title: Binary index-header
type: docs
menu: operating
slug: /binary-index-header.md
---

# Binary index-header

In order to query series inside blocks from object storage, Store Gateway has to know certain initial info about each block such as:

- symbols table to unintern string values
- postings offset for posting lookup

In order to achieve so, on startup for each block `index-header` is built from pieces of original block's index and stored on disk.
Such `index-header` file is then mmaped and used by Store Gateway.

## Format (version 1)

The following describes the format of the `index-header` file found in each block store gateway local directory.
It is terminated by a table of contents which serves as an entry point into the index.

```
┌─────────────────────────────┬───────────────────────────────┐
│    magic(0xBAAAD792) <4b>   │      version(1) <1 byte>      │
├─────────────────────────────┬───────────────────────────────┤
│  index version(2) <1 byte>  │ index PostingOffsetTable <8b> │
├─────────────────────────────┴───────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │      Symbol Table (exact copy from original index)      │ │
│ ├─────────────────────────────────────────────────────────┤ │
│ │      Posting Offset Table (exact copy from index)       │ │
│ ├─────────────────────────────────────────────────────────┤ │
│ │                          TOC                            │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

When the index is written, an arbitrary number of padding bytes may be added between the lined out main sections above. When sequentially scanning through the file, any zero bytes after a section's specified length must be skipped.

Most of the sections described below start with a `len` field. It always specifies the number of bytes just before the trailing CRC32 checksum. The checksum is always calculated over those `len` bytes.

### Symbol Table

See [Symbols](https://github.com/prometheus/prometheus/blob/d782387f814753b0118d402ec8cdbdef01bf9079/tsdb/docs/format/index.md#symbol-table)

### Postings Offset Table

See [Posting Offset Table](https://github.com/prometheus/prometheus/blob/d782387f814753b0118d402ec8cdbdef01bf9079/tsdb/docs/format/index.md#postings-offset-table)

### TOC

The table of contents serves as an entry point to the entire index and points to various sections in the file.
If a reference is zero, it indicates the respective section does not exist and empty results should be returned upon lookup.

```
┌─────────────────────────────────────────┐
│ ref(symbols) <8b>                       │
├─────────────────────────────────────────┤
│ ref(postings offset table) <8b>         │
├─────────────────────────────────────────┤
│ CRC32 <4b>                              │
└─────────────────────────────────────────┘
```

## How the index-header is built

The [Store Gateway](../components/store.md) periodically scans the bucket to look for new and deleted blocks. For each new block found, the Gateway stores the index-header on the local disk, building it with specific sections of the block's index downloaded using GET byte range requests.

## Impact on number of open file descriptors

The Store Gateway stores each block's index-header on the local disk and loads it via mmap. This means that the Gateway keeps a file descriptor for each loaded block. If your Thanos setup has many blocks in the bucket, the Gateway may hit the `file-max` ulimit (maximum number of open file descriptions by a process); in such case, we recommend increasing the limit on your system.
