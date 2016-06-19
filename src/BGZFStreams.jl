module BGZFStreams

export
    VirtualOffset,
    BGZFStream,
    virtualoffset

using Libz
using Compat

if VERSION < v"0.5-"
    const view = sub
end

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
