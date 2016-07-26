module BGZFStreams

export
    BGZFStream,
    VirtualOffset,
    virtualoffset

using Libz
using Base.Threads

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
