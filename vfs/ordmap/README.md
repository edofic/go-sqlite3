# Go `ordmap` SQLite VFS

This package implements the [`"memdb"`](https://sqlite.org/src/doc/tip/src/memdb.c)
SQLite VFS in pure Go as copy-on-write.

It has some benefits over the C version:
- the memory backing the database needs not be contiguous,
- the database can grow/shrink incrementally without copying,
- reader-writer concurrency is slightly improved.

Benchmark results (on Apple M2 Pro):

```
BenchmarkInsert/ordmap/1-10                 2943             35054 ns/op       28527 /s  41%
BenchmarkInsert/memdb/1-10                  8726             14510 ns/op       68918 /s
BenchmarkInsert/ordmap/10-10                1801             64948 ns/op       15397 /s  62%
BenchmarkInsert/memdb/10-10                 2994             40437 ns/op       24730 /s
BenchmarkInsert/ordmap/100-10                456            260317 ns/op        3841 /s  87%
BenchmarkInsert/memdb/100-10                 523            227228 ns/op        4401 /s
BenchmarkInsert/ordmap/1000-10                54           2136472 ns/op         468 /s  97%
BenchmarkInsert/memdb/1000-10                 57           2077542 ns/op         481 /s
BenchmarkQuery/ordmap/1-10                 35326              3393 ns/op      294724 /s  97%
BenchmarkQuery/memdb/1-10                  35860              3292 ns/op      303767 /s
BenchmarkQuery/ordmap/10-10                14443              8342 ns/op      119875 /s  98%
BenchmarkQuery/memdb/10-10                 14692              8197 ns/op      121996 /s
BenchmarkQuery/ordmap/100-10                2204             55424 ns/op       18043 /s  99%
BenchmarkQuery/memdb/100-10                 2126             54953 ns/op       18197 /s
BenchmarkQuery/ordmap/1000-10                212            561834 ns/op        1780 /s 100%
BenchmarkQuery/memdb/1000-10                 212            562003 ns/op        1779 /s
```
