---
title: "Compaction in Streambed"
date: 2026-08-09
authors:
  - name: Vignesh (Viggy) Ravichandran
---

Streambed is getting more mature. Big data has a small problem or more precisely small file problem.

<!--more-->

Every flush (not every single DML) creates a new parquet file. Typically that means hundreds and thousands of small files for a single table. Two challenges with that:

1. It impacts the latency (specifically query read)
2. Maintenance (S3 scales but it's not like infinite)

To solve that we introduce, `maintenance` and `maintenance compact` command which does metadata maintenance, small file compaction.

## Implementation

Reads multiple small files and rewrites them as a large parquet file. Using sqlite for compare and swap. Explored S3 conditional rewites but we will introduce once streambed support multiple hosts. For single host, sqlite row lock is sufficient. Implemented as a separate process to keep the `sync` process clean and also reliable (avoid co-ordination b/w multiple routine). Eventually should merge it as part of the `sync` daemon.

## What's next

Support compaction in MOR [equality delete](https://github.com/viggy28/streambed/issues/32), [optimize COW with key-range pruning](https://github.com/viggy28/streambed/issues/36), 
