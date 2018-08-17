# BGZFStream
# ==========

# Internal details
# ----------------
#
# When reading data from an input, compressed data will be read to a buffer
# (compressed block) and then inflated into a decompressed block at a time.
# When writing data to an output, raw data will be deflated into a compressed
# block and then written to the output immediately.  Each data block is no
# larger than 64 KiB before and after compression.
#
# Read mode (stream.mode = READ_MODE)
# -----------------------------------
#
#          compressed block          decompressed block
# stream   +---------------+         +---------------+
# .io ---> |xxxxxxx        | ------> |xxxxxxxxxxx    | --->
#     read +---------------+ inflate +---------------+ read
#                                    |------>| block.position ∈ [0, 64K)
#                                    |--------->| block.size ∈ [0, 64K]
#
# Write mode (stream.mode = WRITE_MODE)
# -------------------------------------
#
#          compressed block          decompressed block
# stream   +---------------+         +---------------+
# .io <--- |xxxxxxx        | <------ |xxxxxxxx       | <---
#    write +---------------+ deflate +---------------+ write
#                                    |------>| block.position ∈ [0, 64K)
#                                    |------------->| block.size = 64K - 256
# - xxx: used data
# - 64K: 65536 (= BGZF_MAX_BLOCK_SIZE = 64 * 1024)

mutable struct Block
    # space for the compressed block
    compressed_block::Vector{UInt8}

    # space for the decompressed block
    decompressed_block::Vector{UInt8}

    # block offset in a file (this is always 0 for a pipe stream)
    block_offset::Int

    # the next reading byte position in a block
    position::Int

    # number of available bytes in the decompressed block
    size::Int

    # zstream object
    zstream::CodecZlib.ZStream
end

function Block(mode)
    compressed_block = Vector{UInt8}(undef, BGZF_MAX_BLOCK_SIZE)
    decompressed_block = Vector{UInt8}(undef, BGZF_MAX_BLOCK_SIZE)

    zstream = CodecZlib.ZStream()
    if mode == READ_MODE
        windowbits = 32 + 15
        CodecZlib.inflate_init!(zstream, windowbits)
        size = 0
    else
        windowbits = 16 + 15
        CodecZlib.deflate_init!(zstream, CodecZlib.Z_DEFAULT_COMPRESSION, windowbits)
        size = BGZF_SAFE_BLOCK_SIZE
    end

    return Block(compressed_block, decompressed_block, 0, 1, size, zstream)
end

# Stream type for the BGZF compression format.
mutable struct BGZFStream{T<:IO} <: IO
    # underlying IO stream
    io::T

    # read/write mode
    mode::UInt8

    # compressed & decompressed blocks with metadata and zstream
    blocks::Vector{Block}

    # current block index
    block_index::Int

    # whether stream is open
    isopen::Bool

    # callback function called when closing the stream
    onclose::Function
end

# BGZF blocks are no larger than 64 KiB before and after compression.
const BGZF_MAX_BLOCK_SIZE = UInt(64 * 1024)

# BGZF_MAX_BLOCK_SIZE minus "margin for safety"
# NOTE: Data block will become slightly larger after deflation when bytes are
# randomly distributed.
const BGZF_SAFE_BLOCK_SIZE = UInt(BGZF_MAX_BLOCK_SIZE - 256)

# Read mode:  inflate and read a BGZF file
# Write mode: deflate and write a BGZF file
const READ_MODE  = 0x00
const WRITE_MODE = 0x01

"""
    BGZFStream(io::IO[, mode::AbstractString="r"])
    BGZFStream(filename::AbstractString[, mode::AbstractString="r"])

Create an I/O stream for the BGZF compression format.

The first argument is either an `IO` object or a filename. If `mode` is `"r"`
(read) the BGZF stream will be in read mode and decompress the underlying BGZF
blocks while reading. In read mode, `BGZFStream` supports the `seek` operation
using a virtual file offset (see `VirtualOffset`). If `mode` is `"w"` (write)
or `"a"` (append) the BGZF stream will be in write mode and compress written
data to BGZF blocks.
"""
function BGZFStream(io::IO, mode::AbstractString="r")
    if mode ∉ ("r", "w", "a")
        throw(ArgumentError("invalid mode: \"", mode, "\""))
    end

    # the number of parallel workers
    mode′ = mode == "r" ? READ_MODE : WRITE_MODE
    if mode′ == READ_MODE
        blocks = [Block(mode′) for _ in 1:Threads.nthreads()]
    else
        # Write mode is not (yet?) multi-threaded.
        blocks = [Block(mode′)]
    end
    return BGZFStream(io, mode′, blocks, 1, true, io -> close(io))
end

function BGZFStream(filename::AbstractString, mode::AbstractString = "r")
    if mode ∉ ("r", "w", "a")
        throw(ArgumentError("invalid mode: '", mode, "'"))
    end
    return BGZFStream(open(filename, mode), mode)
end

function Base.open(::Type{BGZFStream}, filepath::AbstractString, mode::AbstractString = "r")
    return BGZFStream(filepath, mode)
end

"""
    virtualoffset(stream::BGZFStream)

Return the current virtual file offset of `stream`.
"""
function virtualoffset(stream::BGZFStream)
    if stream.mode == READ_MODE
        i = ensure_buffered_data(stream)
        if i == 0
            block = stream.blocks[stream.block_index]
        else
            block = stream.blocks[i]
        end
    else
        block = stream.blocks[1]
    end
    return VirtualOffset(block.block_offset, block.position - 1)
end

function virtualoffset(stream::BGZFStream{T}) where {T<:Base.AbstractPipe}
    throw(ArgumentError("virtualoffset is not supported for a pipe stream"))
end

function Base.show(io::IO, stream::BGZFStream)
    print(io,
        summary(stream),
        "(<",
        "mode=", stream.mode == READ_MODE ? "read" : "write",
        ">)")
end

function Base.isopen(stream::BGZFStream)
    return stream.isopen
end

function Base.close(stream::BGZFStream)
    if stream.mode == WRITE_MODE
        if stream.blocks[1].position > 1
            write_blocks!(stream)
        end
        write(stream.io, EOF_BLOCK)
    end
    for block in stream.blocks
        end_zstream(block.zstream, stream.mode)
    end
    stream.isopen = false
    stream.onclose(stream.io)
    return
end

function Base.flush(stream::BGZFStream)
    if stream.mode == WRITE_MODE
        flush(stream.io)
    end
    return
end

function Base.eof(stream::BGZFStream)
    if stream.mode == READ_MODE
        return ensure_buffered_data(stream) == 0
    else
        return true
    end
end

function Base.seekstart(stream::BGZFStream)
    seek(stream, VirtualOffset(0, 0))
end

function Base.seek(stream::BGZFStream, voffset::VirtualOffset)
    if stream.mode == WRITE_MODE
        throw(ArgumentError("BGZFStream in write mode is not seekable"))
    end
    block_offset, inblock_offset = offsets(voffset)
    seek(stream.io, block_offset)
    read_blocks!(stream)
    block = first(stream.blocks)
    if inblock_offset ≥ block.size
        throw(ArgumentError("too large in-block offset"))
    end
    block.block_offset = block_offset
    block.position = inblock_offset + 1
    return
end

function Base.seek(stream::BGZFStream{T}, voffset::VirtualOffset) where {T<:Base.AbstractPipe}
    throw(ArgumentError("seek is not supported for a pipe stream"))
end

function Base.read(stream::BGZFStream, ::Type{UInt8})
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != READ_MODE
        throw(ArgumentError("stream is not readable"))
    end
    block_index = ensure_buffered_data(stream)
    if block_index == 0
        throw(EOFError())
    end
    block = stream.blocks[block_index]
    byte = block.decompressed_block[block.position]
    block.position += 1
    return byte
end

function Base.write(stream::BGZFStream, byte::UInt8)
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != WRITE_MODE
        throw(ArgumentError("stream is not writable"))
    end
    block = stream.blocks[1]
    block.decompressed_block[block.position] = byte
    block.position += 1
    if block.position > block.size
        write_blocks!(stream)
    end
    return 1
end

function Base.unsafe_read(stream::BGZFStream, p::Ptr{UInt8}, n::UInt)
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != READ_MODE
        throw(ArgumentError("stream is not readable"))
    end
    p_end = p + n
    while p < p_end
        i = ensure_buffered_data(stream)
        if i == 0
            throw(EOFError())
        end
        @inbounds block = stream.blocks[i]
        len = min(p_end - p, block.size - block.position + 1)
        src = pointer(block.decompressed_block, block.position)
        memcpy(p, src, len)
        block.position += len
        p += len
    end
end

function Base.unsafe_write(stream::BGZFStream, p::Ptr{UInt8}, n::UInt)
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != WRITE_MODE
        throw(ArgumentError("stream is not writable"))
    end
    block = stream.blocks[1]
    p_end = p + n
    while p < p_end
        len = min(p_end - p, block.size - block.position + 1)
        dst = pointer(block.decompressed_block, block.position)
        memcpy(dst, p, len)
        block.position += len
        if block.position > block.size
            write_blocks!(stream)
        end
        p += len
    end
    return Int(n)
end


# Internal functions
# ------------------

# Ensure buffered data (at least 1 byte) for reading and return the block index
# if available or 0 otherwise.
@inline function ensure_buffered_data(stream)
    #@assert stream.mode == READ_MODE
    @label doit
    while stream.block_index ≤ lastindex(stream.blocks)
        @inbounds block = stream.blocks[stream.block_index]
        if block.position ≤ block.size
            return stream.block_index
        end
        stream.block_index += 1
    end
    if !eof(stream.io)
        read_blocks!(stream)
        @goto doit
    end
    return 0
end

# A wrapper of memcpy.
function memcpy(dst, src, len)
    ccall(
        :memcpy,
        Ptr{Cvoid},
        (Ptr{Cvoid}, Ptr{Cvoid}, Csize_t),
        dst, src, len)
end

struct BGZFDataError <: Exception
    message::AbstractString
end

# Throw a BGZFDataError exception with the given error message.
function bgzferror(message::AbstractString="malformed BGZF data")
    throw(BGZFDataError(message))
end

# Read and inflate blocks.
function read_blocks!(stream)
    @assert stream.mode == READ_MODE

    # read BGZF blocks in sequence
    n_blocks = 0
    has_position = true
    try
        position(stream.io)
    catch
        has_position = false
    end
    while n_blocks < length(stream.blocks) && !eof(stream.io)
        block = stream.blocks[n_blocks += 1]
        if has_position
            block.block_offset = position(stream.io)
        end
        block.position = 1
        bsize = read_bgzf_block!(stream.io, block.compressed_block)
        zstream = block.zstream
        zstream.next_in = pointer(block.compressed_block)
        zstream.avail_in = bsize
        zstream.next_out = pointer(block.decompressed_block)
        zstream.avail_out = BGZF_MAX_BLOCK_SIZE
    end

    # inflate blocks in parallel
    rets = Vector{Cint}(undef, n_blocks)
    Threads.@threads for i in 1:n_blocks
        block = stream.blocks[i]
        zstream = block.zstream
        old_avail_out = zstream.avail_out
        rets[i] = CodecZlib.inflate!(zstream, CodecZlib.Z_FINISH)
        block.size = old_avail_out - zstream.avail_out
    end

    for i in 1:n_blocks
        if rets[i] != CodecZlib.Z_STREAM_END
            error("zlib failed to inflate a compressed block")
        end
        block = stream.blocks[i]
        @assert block.size ≤ BGZF_MAX_BLOCK_SIZE
        reset_zstream(block.zstream, stream.mode)
    end

    stream.block_index = 1
    return
end

# Read a BGZF block from `input`.
function read_bgzf_block!(input, block)
    # TODO: check the number of read bytes

    # +---+---+---+---+---+---+---+---+---+---+
    # |ID1|ID2|CM |FLG|     MTIME     |XFL|OS | (more-->)
    # +---+---+---+---+---+---+---+---+---+---+
    unsafe_read(input, pointer(block), 10)
    id1_ok = block[1] == 0x1f
    id2_ok = block[2] == 0x8b
    cm_ok  = block[3] == 0x08
    flg_ok = block[4] == 0x04
    if !id1_ok || !id2_ok
        bgzferror("invalid gzip identifier")
    elseif !cm_ok
        bgzferror("invalid compression method")
    elseif !flg_ok
        bgzferror("invalid flag")
    end

    # +---+---+=================================+
    # | XLEN  |...XLEN bytes of "extra field"...| (more-->)
    # +---+---+=================================+
    unsafe_read(input, pointer(block, 11) , 2)
    xlen = UInt16(block[11]) | UInt16(block[12]) << 8
    unsafe_read(input, pointer(block, 13), xlen)
    bsize::Int = 0
    pos = 12
    while pos < 12 + xlen
        si1 = block[pos+1]
        si2 = block[pos+2]
        slen = UInt16(block[pos+3]) | UInt16(block[pos+4]) << 8
        if si1 == 0x42 || si2 == 0x43
            if slen != 2
                bgzferror("invalid subfield length")
            end
            bsize = (UInt16(block[pos+5]) | UInt16(block[pos+6]) << 8) + 1
        end
        # skip this field
        pos += 4 + slen
    end
    if bsize == 0
        bgzferror("no block size")
    end

    # +=======================+---+---+---+---+---+---+---+---+
    # |...compressed blocks...|     CRC32     |     ISIZE     |
    # +=======================+---+---+---+---+---+---+---+---+
    size = bsize - 1 - xlen - 19 + 8
    unsafe_read(input, pointer(block, 13 + xlen), size)

    if eof(input) && !is_eof_block(block)
        bgzferror("no end-of-file marker (maybe a truncated file)")
    end

    return bsize
end

function write_blocks!(stream)
    @assert stream.mode == WRITE_MODE

    n_blocks = length(stream.blocks)
    @assert n_blocks == 1

    for i in 1:n_blocks
        block = stream.blocks[i]
        zstream = block.zstream
        zstream.next_in = pointer(block.decompressed_block)
        zstream.avail_in = block.position - 1
        zstream.next_out = pointer(block.compressed_block, 9)
        zstream.avail_out = BGZF_MAX_BLOCK_SIZE - 8

        ret = CodecZlib.deflate!(zstream, CodecZlib.Z_FINISH)
        if ret != CodecZlib.Z_STREAM_END
            if ret == CodecZlib.Z_OK
                error("block size may exceed BGZF_MAX_BLOCK_SIZE")
            else
                error("failed to compress a BGZF block (zlib error $(ret))")
            end
        end

        blocksize = (BGZF_MAX_BLOCK_SIZE - 8) - zstream.avail_out + 8
        fix_header!(block.compressed_block, blocksize)
        nb = unsafe_write(stream.io, pointer(block.compressed_block), blocksize)
        if nb != blocksize
            error("failed to write a BGZF block")
        end
        if !isa(stream.io, Pipe)
            block.block_offset = position(stream.io)
        end
        block.position = 1

        reset_zstream(zstream, WRITE_MODE)
    end
end

function fix_header!(block, blocksize)
    copyto!(block,
            # ID1   ID2    CM   FLG  |<--     MTIME    -->|   XFL    OS
            [0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff])
    copyto!(block, 11,
            #  XLEN    S1    S2    SLEN          BSIZE
            reinterpret(UInt8, [0x0006, 0x4342, 0x0002, UInt16(blocksize - 1)]))
end

# end-of-file marker block (used for detecting unintended file truncation)
const EOF_BLOCK = [
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
    0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00
]

# Return true iff the block is a end-of-file marker.
function is_eof_block(block)
    if length(block) < length(EOF_BLOCK)
        return false
    end
    for i in 1:lastindex(EOF_BLOCK)
        if block[i] != EOF_BLOCK[i]
            return false
        end
    end
    return true
end

# Reset the zstream.
function reset_zstream(zstream, mode)
    if mode == READ_MODE
        @assert CodecZlib.inflate_reset!(zstream) == CodecZlib.Z_OK
    else
        @assert CodecZlib.deflate_reset!(zstream) == CodecZlib.Z_OK
    end
    return
end

# End the zstream.
function end_zstream(zstream, mode)
    if mode == READ_MODE
        @assert CodecZlib.inflate_end!(zstream) == CodecZlib.Z_OK
    else
        @assert CodecZlib.deflate_end!(zstream) == CodecZlib.Z_OK
    end
    return
end
