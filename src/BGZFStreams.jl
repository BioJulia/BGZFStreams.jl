module BGZFStreams

export
    VirtualOffset,
    BGZFStream,
    virtualoffset,
    Tabix,
    overlapchunks

using Libz
using Base.Threads

include("virtualoffset.jl")
include("bgzfstream.jl")
include("chunk.jl")
include("tabix.jl")

end # module
