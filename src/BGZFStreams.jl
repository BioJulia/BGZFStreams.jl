module BGZFStreams

export
    VirtualOffset,
    BGZFStream,
    virtualoffset

using Libz

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
