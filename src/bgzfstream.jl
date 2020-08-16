# Note: The code in this file would be much simpler and also faster if,
# instead of the codecs keeping their own buffer, it simply operated on the
# TranscodingStream buffer. However, I've not been able to figure out how to
# ensure that the buffer has at least N bytes available, which is necessary
# for a block compression algorithm

# Todo: Paralellize de/compression

function BGZFCompressorStream(io::IO, compresslevel::Int=6)
    N = Threads.nthreads()
    codec = BGZFCompressor(io, N, compresslevel)
    bufsize = N * (1 << 16)
    return TranscodingStream(codec, io; bufsize=bufsize)
end

function BGZFDecompressorStream(io::IO)
    N = Threads.nthreads()
    codec = BGZFDecompressor(N)
    bufsize = N * (1 << 16)
    return TranscodingStream(codec, io; bufsize=bufsize)
end    

abstract type BGZFCodec <: TranscodingStreams.Codec end

nblocks(c::BGZFCodec) = length(c.blocks)

mutable struct BGZFCompressor{T <: IO} <: BGZFCodec
    # We only keep this so we can write an EOF stream on close.
    stream::T
    compressors::Vector{Compressor}
    inbuffer::Vector{UInt8}
    outbuffer::Vector{UInt8}
    blocks::Vector{Block}
    outpos::Int
    outlen::Int
    inlen::Int
end

function BGZFCompressor(stream::IO, nthreads, compresslevel)
    c = [Compressor(compresslevel) for i in 1:nthreads]
    nbytes = (1 << 16) * nthreads
    a = Vector{UInt8}(undef, nbytes)
    b = Vector{UInt8}(undef, nbytes)
    blocks = Vector{Block}(undef, nthreads)
    return BGZFCompressor{typeof(stream)}(stream, c, a, b, blocks, 1, 0, 0)
end

mutable struct BGZFDecompressor <: BGZFCodec
    decompressors::Vector{Decompressor}
    inbuffer::Vector{UInt8}
    outbuffer::Vector{UInt8}
    blocks::Vector{Block}
    outpos::Int
    outlen::Int
    inlen::Int
end

function BGZFDecompressor(nthreads)
    c = [Decompressor() for i in 1:nthreads]
    nbytes = (1 << 16) * nthreads
    a = Vector{UInt8}(undef, nbytes)
    b = Vector{UInt8}(undef, nbytes)
    blocks = Vector{Block}(undef, nthreads)
    return BGZFDecompressor(c, a, b, blocks, 1, 0, 0)
end

full_buffersize(d::BGZFDecompressor) = (1 << 16) * length(d.c)

function copy_from_outbuffer(codec::BGZFCodec, output::Memory, consumed::Integer)
    n = min(codec.outlen - codec.outpos + 1, length(output))
    unsafe_copyto!(output.ptr, pointer(codec.outbuffer, codec.outpos), n)
    codec.outpos += n
    if codec.outpos == codec.outlen
        codec.outpos = 1
        codec.outlen = 0
    end
    return (Int(consumed), n, :ok)
end

function _process(codec::BGZFCodec, input, output, error, bytes_per_block)
    consumed = 0
    
    # If we have spare data in buffer, just give that
    if codec.outpos <= codec.outlen
        return copy_from_outbuffer(codec, output, consumed)
    end    

    # If no more data anywhere (outbuffer, inbuffer or input), end stream
    if iszero(length(input)) & iszero(codec.inlen)
        return (0, 0, :end)
    end

    # Read in bytes from input if possible
    filled_bufferlen = nblocks(codec) * bytes_per_block
    if !iszero(length(input))
        bytes_space = filled_bufferlen - codec.inlen
        consumed = min(bytes_space, length(input))
        unsafe_copyto!(pointer(codec.inbuffer, codec.inlen + 1), input.ptr, consumed)
        codec.inlen += consumed
    end

    # If not end of input stream and inbuffer not filled, just return zero bytes,
    # and wait untill we have read more from input before decompressing
    if (length(input) > 0) & (codec.inlen < filled_bufferlen)
        return (consumed, 0, :ok)
    end
    return (consumed, 0, :process)
end

function TranscodingStreams.process(codec::BGZFDecompressor,
                                    input::Memory, output::Memory, error::Error)
    consumed, produced, status = _process(codec, input, output, error, MAX_BLOCK_SIZE)
    if (status === :ok) | (status === :end)
        return (consumed, produced, status)
    else
        has_last_block = iszero(length(input))
        decompress!(codec, has_last_block)
        return copy_from_outbuffer(codec, output, consumed)
    end
end

function decompress!(codec::BGZFDecompressor, has_last_block::Bool)
    # First index as many blocks as we can - either until no more data, or
    # at most nblocks(codec) blocks
    N = 0
    consumed = 0
    decompress_pos = 1
    for i in 1:nblocks(codec)
        block = index!(codec.inbuffer, consumed+1, codec.inlen, decompress_pos)
        codec.blocks[i] = block
        N += 1
        consumed += block.blocklen
        decompress_pos += block.decompress_len

        if consumed >= codec.inlen
            if has_last_block
                check_eof_block(block, codec.inbuffer, consumed - block.blocklen + 1)
            end
            break
        end
    end
    
    Threads.@threads for i in 1:N
        decompress!(codec.decompressors[i], codec.blocks[i], codec.outbuffer, codec.inbuffer)
    end
    codec.outlen = decompress_pos - 1
    codec.outpos = 1

    # Finally shift data in inbuffer
    copyto!(codec.inbuffer, 1, codec.inbuffer, consumed+1, codec.inlen - consumed)
    codec.inlen -= consumed
end

function TranscodingStreams.process(codec::BGZFCompressor,
                                    input::Memory, output::Memory, error::Error)
    consumed, produced, status = _process(codec, input, output, error, SAFE_BLOCK_SIZE)
    if (status === :ok) | (status === :end)
        return (consumed, produced, status)
    else
        compress!(codec)
        return copy_from_outbuffer(codec, output, consumed)
    end
end

function compress!(codec::BGZFCompressor)
    N = min(nblocks(codec), cld(codec.inlen, SAFE_BLOCK_SIZE))

    Threads.@threads for i in 1:N
        len = min(codec.inlen - (i-1)*SAFE_BLOCK_SIZE, SAFE_BLOCK_SIZE)
        codec.blocks[i] = compress_block!(codec.compressors[i], codec.inbuffer,
                          codec.outbuffer, i, len)
    end

    # Move data together densely. Begin at 19th byte to make room for header
    pos = 1
    for i in 1:N
        pos += copy_block!(codec.outbuffer, codec.blocks[i], pos)
    end
    codec.outlen = pos - 1
    codec.outpos = 1
    codec.inlen -= min(N * SAFE_BLOCK_SIZE, codec.inlen)
end

function TranscodingStreams.finalize(codec::BGZFCompressor)
    write(codec.stream, EOF_BLOCK)
end
