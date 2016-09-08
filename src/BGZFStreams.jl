module BGZFStreams

export
    BGZFStream,
    VirtualOffset,
    BGZFDataError,
    virtualoffset

using Libz
using Base.Threads

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
