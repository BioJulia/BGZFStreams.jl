# BGZFStreams

[![Build Status](https://travis-ci.org/BioJulia/BGZFStreams.jl.svg?branch=master)](https://travis-ci.org/BioJulia/BGZFStreams.jl)
[![codecov.io](http://codecov.io/github/BioJulia/BGZFStreams.jl/coverage.svg?branch=master)](http://codecov.io/github/BioJulia/BGZFStreams.jl?branch=master)

BGZF is a compression format that supports random access using *virtual file offsets*.

See the SAM/BAM file format specs for the details of BGZF: <https://samtools.github.io/hts-specs/SAMv1.pdf>.


## Synopsis

```julia
using BGZFStreams

# The first argument is a filename or an IO object (e.g. IOStream).
stream = BGZFStream("data.bgz")

# BGZFStream is a subtype of IO and works like a usual IO object.
while !eof(stream)
    byte = read(stream, UInt8)
    # do something...
end

# BGZFStream is also seekable with a VirtualOffset.
seek(stream, VirtualOffset(0, 2))

# The current virtual file offset is available.
virtualoffset(stream)

close(stream)
```


## Usage

The BGZFStreams.jl package exports three types and a function to the package user:

* Types:
    * `BGZFStream`: an `IO` stream of the BGZF file format
    * `VirtualOffset`: data offset in a BGZF file
    * `BGZFDataError`: an error type thrown when reading a malformed byte stream
* Function:
    * `virtualoffset(stream)`: returns the current virtual file offset of `stream`

The `BGZFStream` type wraps an underlying `IO` object and transparently inflate
(for reading) or deflate (for writing) data. Since it is a subtype of `IO`, an
instance of it behaves like other `IO` objects, but the `seek` method takes a
virtual offset instead of a normal file offset as its second argument.

The `VirtualOffset` type represents a 64-bit virtual file offset as described in
the specification of the SAM file format. That is, the most significant 48-bit
integer of a virtual offset is a byte offset to the BGZF file to the beginning
position of a BGZF block and the least significant 16-bit integer is a byte
offset to the uncompressed byte(s).

The `BGZFDataError` type is a subtype of `Exception` and used to throw an
exception when invalid data are read.

The `virtualoffset(stream::BGZFStream)` returns the current virtual file offset.
More specifically, it returns the virtual offset of the next reading byte while
reading and the next writing byte while writing.


## Parallel Processing

A major bottleneck of processing a BGZF file is the inflation and deflation
process. The throughput of reading data is ~100 MiB/s, which is quite slower
than that of raw reading from a file. In order to alleviate this problem, this
package supports parallelized inflation when reading compressed data. This
requires the support of multi-threading introduced in Julia 0.5. The
`JULIA_NUM_THREADS` environmental variable sets the number of threads used for
processing.

    bash-3.2$ JULIA_NUM_THREADS=2 julia -q
    julia> using Base.Threads

    julia> nthreads()
    2
