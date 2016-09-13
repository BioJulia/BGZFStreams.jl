module BGZFStreams

export
    BGZFStream,
    VirtualOffset,
    BGZFDataError,
    virtualoffset

import Libz

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
