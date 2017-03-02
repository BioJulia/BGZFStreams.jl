__precompile__()

module BGZFStreams

export
    BGZFStream,
    VirtualOffset,
    BGZFDataError,
    virtualoffset

import Compat: @compat
import Libz

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
