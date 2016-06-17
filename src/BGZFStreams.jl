module BGZFStreams

export
    VirtualOffset,
    BGZFStream

using Libz

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
