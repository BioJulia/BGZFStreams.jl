# BGZFStreams

[![Build Status](https://travis-ci.org/bicycle1885/BGZFStreams.jl.svg?branch=master)](https://travis-ci.org/bicycle1885/BGZFStreams.jl)

BGZF is compression format that supports random access using *virtual file offsets*.

See the SAM/BAM file format specs for the details: <https://samtools.github.io/hts-specs/SAMv1.pdf>.


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

close(stream)
```
