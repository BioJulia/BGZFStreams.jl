module BGZFStreams

export
    BGZFStream,
    VirtualOffset,
    BGZFDataError,
    virtualoffset

import CodecZlib

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
