module BGZFStreams

export
    VirtualOffset,
    BGZFStream,
    virtualoffset

using Libz
using Base.Threads

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
