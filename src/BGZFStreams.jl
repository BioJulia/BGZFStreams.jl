module BGZFStreams

export
    VirtualOffset,
    BGZFStream,
    virtualoffset

using Libz
using Compat

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
