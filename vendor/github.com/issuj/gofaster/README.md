# gofaster
Amd64 asm optimized alternatives for some Go stdlib packages

I needed these for another project. Since they're general purpose, I decided to publish them as a separate repo.

Contents, so far:
* Base64 encoder, interface similar to "encoding/base64"
* Crc32 with Kadatch & Jenkins (aka crcutil) algorithm, interface similar to "hash/crc32" (*)

(*) You can find a much faster Crc32 lib here: https://github.com/klauspost/crc32 (also in Go 1.6 stdlib)

Base64 benchmark result (vs stdlib), 3*16k block size:
```
BenchmarkStdlib    10000            159637 ns/op         307.90 MB/s
BenchmarkSimd     100000             13612 ns/op        3610.81 MB/s
```

Crc32 benchmark result (vs stdlib), 16k block size:
```
BenchmarkStdlib    30000             46786 ns/op         350.85 MB/s
BenchmarkKandJ    300000              5858 ns/op        2801.91 MB/s
```

Benchmarks were run on an Intel Core i7-5600U (Broadwell, 3.2GHz), Go version 1.4.2
