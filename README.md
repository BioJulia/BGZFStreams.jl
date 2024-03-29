# BGZFStreams

[![Unit Tests](https://github.com/BioJulia/BGZFStreams.jl/actions/workflows/UnitTests.yml/badge.svg?branch=master)](https://github.com/BioJulia/BGZFStreams.jl/actions/workflows/UnitTests.yml)
[![codecov.io](http://codecov.io/github/BioJulia/BGZFStreams.jl/coverage.svg?branch=master)](http://codecov.io/github/BioJulia/BGZFStreams.jl?branch=master)
[![Downstream](https://github.com/BioJulia/BGZFStreams.jl/actions/workflows/Downstream.yml/badge.svg)](https://github.com/BioJulia/BGZFStreams.jl/actions/workflows/Downstream.yml)


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

## Contributing
We appreciate [contributions](https://github.com/BioJulia/BGZFStreams.jl/graphs/contributors) from users including reporting bugs, fixing issues, improving performance and adding new features.

Take a look at the [contributing files](https://github.com/BioJulia/Contributing) detailed contributor and maintainer guidelines, and code of conduct.

### Financial contributions
We also welcome financial contributions in full transparency on our [open collective](https://opencollective.com/biojulia).
Anyone can file an expense.
If the expense makes sense for the development the core contributors and the person who filed the expense will be reimbursed.


## Backers & Sponsors
Thank you to all our backers and sponsors!

Love our work and community? [Become a backer](https://opencollective.com/biojulia#backer).

[![backers](https://opencollective.com/biojulia/backers.svg?width=890)](https://opencollective.com/biojulia#backers)

Does your company use BioJulia?
Help keep BioJulia feature rich and healthy by [sponsoring the project](https://opencollective.com/biojulia#sponsor).
Your logo will show up here with a link to your website.

[![](https://opencollective.com/biojulia/sponsor/0/avatar.svg)](https://opencollective.com/biojulia/sponsor/0/website)
[![](https://opencollective.com/biojulia/sponsor/1/avatar.svg)](https://opencollective.com/biojulia/sponsor/1/website)
[![](https://opencollective.com/biojulia/sponsor/2/avatar.svg)](https://opencollective.com/biojulia/sponsor/2/website)
[![](https://opencollective.com/biojulia/sponsor/3/avatar.svg)](https://opencollective.com/biojulia/sponsor/3/website)
[![](https://opencollective.com/biojulia/sponsor/4/avatar.svg)](https://opencollective.com/biojulia/sponsor/4/website)
[![](https://opencollective.com/biojulia/sponsor/5/avatar.svg)](https://opencollective.com/biojulia/sponsor/5/website)
[![](https://opencollective.com/biojulia/sponsor/6/avatar.svg)](https://opencollective.com/biojulia/sponsor/6/website)
[![](https://opencollective.com/biojulia/sponsor/7/avatar.svg)](https://opencollective.com/biojulia/sponsor/7/website)
[![](https://opencollective.com/biojulia/sponsor/8/avatar.svg)](https://opencollective.com/biojulia/sponsor/8/website)
[![](https://opencollective.com/biojulia/sponsor/9/avatar.svg)](https://opencollective.com/biojulia/sponsor/9/website)


## Questions?
If you have a question about contributing or using BioJulia software, come on over and chat to us on [the Julia Slack workspace](https://julialang.org/slack/), or you can try the [Bio category of the Julia discourse site](https://discourse.julialang.org/c/domain/bio).
