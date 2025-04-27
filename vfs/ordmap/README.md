# Go `ordmap` SQLite VFS

This package implements the [`"memdb"`](https://sqlite.org/src/doc/tip/src/memdb.c)
SQLite VFS in pure Go as copy-on-write.

It has some benefits over the C version:
- the memory backing the database needs not be contiguous,
- the database can grow/shrink incrementally without copying,
- reader-writer concurrency is slightly improved.
- instant forking of the database (while write performance is only slightly slower)

Benchmark results (on Apple M2 Pro):

| Benchmark                       | Time (ns/op)   | Operations/s | Relative speed |
|---------------------------------|----------------|--------------|----------------|
| BenchmarkInsert/ordmap/1-10     |    35054 ns/op |     28527 /s |  41%           |
| BenchmarkInsert/memdb/1-10      |    14510 ns/op |     68918 /s |                |
| BenchmarkInsert/ordmap/10-10    |    64948 ns/op |     15397 /s |  62%           |
| BenchmarkInsert/memdb/10-10     |    40437 ns/op |     24730 /s |                |
| BenchmarkInsert/ordmap/100-10   |   260317 ns/op |      3841 /s |  87%           |
| BenchmarkInsert/memdb/100-10    |   227228 ns/op |      4401 /s |                |
| BenchmarkInsert/ordmap/1000-10  |  2136472 ns/op |       468 /s |  97%           |
| BenchmarkInsert/memdb/1000-10   |  2077542 ns/op |       481 /s |                |
| BenchmarkQuery/ordmap/1-10      |     3393 ns/op |    294724 /s |  97%           |
| BenchmarkQuery/memdb/1-10       |     3292 ns/op |    303767 /s |                |
| BenchmarkQuery/ordmap/10-10     |     8342 ns/op |    119875 /s |  98%           |
| BenchmarkQuery/memdb/10-10      |     8197 ns/op |    121996 /s |                |
| BenchmarkQuery/ordmap/100-10    |    55424 ns/op |     18043 /s |  99%           |
| BenchmarkQuery/memdb/100-10     |    54953 ns/op |     18197 /s |                |
| BenchmarkQuery/ordmap/1000-10   |   561834 ns/op |      1780 /s | 100%           |
| BenchmarkQuery/memdb/1000-10    |   562003 ns/op |      1779 /s |                |

Forking:

| Benahcmark     | Time (ns/op) | Bytes Allocated | Allocations |
|----------------|--------------|-----------------|-------------|
|BenchmarkFork-10|  173.9 ns/op |    147 B/op     | 1 allocs/op |
