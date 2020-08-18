module BGZFStreams

export
    BGZFCompressorStream,
    BGZFDecompressorStream,
    BGZFError,
    VirtualOffset

using LibDeflate
using TranscodingStreams

import TranscodingStreams:
    TranscodingStreams,
    TranscodingStream,
    Memory,
    Error


struct BGZFError <: Exception
    message::String
end

@noinline bgzferror(s::String) = throw(BGZFError(s))

include("virtualoffset.jl")
include("block.jl")
include("bgzfstream.jl")

end # module
