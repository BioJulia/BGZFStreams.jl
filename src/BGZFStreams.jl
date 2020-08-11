module BGZFStreams

export
    BGZFStream,
    VirtualOffset,
    BGZFDataError,
    virtualoffset

using LibDeflate
import Base.Threads: @spawn

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

# end-of-file marker block (used for detecting unintended file truncation)
const EOF_BLOCK = [
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
    0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00
]

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

# Virtual Offset
# ==============
struct VirtualOffset
    x::UInt64
end

Base.convert(::Type{VirtualOffset}, x::UInt64) = VirtualOffset(x)
Base.convert(::Type{UInt64}, x::VirtualOffset) = x.x

"""
    VirtualOffset(block_offset::Integer, inblock_offset::Integer)

Create a virtual file offset from `block_offset` and `inblock_offset`.

`block_offset` is an offset pointing to the beginning position of a BGZF block
in a BGZF file and `inblock_offset` is an offset pointing to the begining
position of a binary data within a uncompressed BGZF block. These values are
zero-based and their valid ranges are [0, 1 << 48) and [0, 1 << 16),
respectively.
"""
function VirtualOffset(block_offset::Integer, inblock_offset::Integer)
    if !(0 ≤ block_offset < (1 << 48))
        throw(ArgumentError("block file offset must be in [0, $(1 << 48))"))
    elseif !(0 ≤ inblock_offset < (1 << 16))
        throw(ArgumentError("in-block offset must be in [0, $(1 << 16))"))
    end
    return VirtualOffset((UInt64(block_offset) << 16) | UInt64(inblock_offset))
end

Base.isless(x::VirtualOffset, y::VirtualOffset) = isless(x.x, y.x)

# NOTE: This doesn't check the valid range of virtual offset.
Base.:+(voffset::VirtualOffset, x::Integer) = VirtualOffset(voffset.x + UInt64(x))
Base.getindex(voffset::VirtualOffset, i::Integer) = offsets(voffset)[i]

offsets(x::VirtualOffset) = (x.x >>> 16, x.x & 0xffff)
Base.show(io::IO, x::VirtualOffset) = print(io, summary(x), offsets(x))
Base.read(io::IO, ::Type{VirtualOffset}) = VirtualOffset(read(io, UInt64))
Base.write(io::IO, voffset::VirtualOffset) = write(io, voffset.x)

# BGZF Block
# ==========
mutable struct Block
    # space for the compressed block
    block_data::Vector{UInt8}

    # space for the decompressed data
    decompressed_data::Vector{UInt8}

    # block offset in a file (this is always 0 for a pipe stream)
    block_offset::Int

    # the next reading position in a block - we don't need a writing position,
    # since we always write to decompressed_data_size + 1
    reading_position::Int32

    # Position within compressed block where the actual compressed data is
    compressed_data_position::Int32
    compressed_data_size::Int32

    decompressed_data_size::Int32

    # de/compressor - one per block permits multithreading
    de_compressor::Union{Compressor, Decompressor}
end

function Block(mode)
    block_data = Vector{UInt8}(undef, BGZF_MAX_BLOCK_SIZE)
    decompressed_data = Vector{UInt8}(undef, BGZF_MAX_BLOCK_SIZE)

    if mode == READ_MODE
        de_compressor = LibDeflate.Decompressor()
    else
        de_compressor = LibDeflate.Compressor()
    end

    return Block(block_data, decompressed_data, 0, 1, 0, 0, 0, de_compressor)
end

# After compressed data we have CRC checksum and ISIZE fields, both 4 bytes
function block_data_size(block::Block)
    return block.compressed_data_position + block.compressed_data_size + 8 - 1
end

remaining_writespace(block::Block) = BGZF_SAFE_BLOCK_SIZE - block.decompressed_data_size

# Read a whole BGZF block from `input`, but do not decompress it
function read_bgzf_block!(input::IO, block::Block)
    data = block.block_data
    block.reading_position = 1

    # +---+---+---+---+---+---+---+---+---+---+---+---+
    # |ID1|ID2|CM |FLG|     MTIME     |XFL|OS | XLEN  | (more-->)
    # +---+---+---+---+---+---+---+---+---+---+---+---+
    unsafe_read(input, pointer(data), 12)
    id1_ok = data[1] == 0x1f
    id2_ok = data[2] == 0x8b
    cm_ok  = data[3] == 0x08
    flg_ok = data[4] == 0x04
    if !id1_ok || !id2_ok
        bgzferror("invalid gzip identifier")
    elseif !cm_ok
        bgzferror("invalid compression method")
    elseif !flg_ok
        bgzferror("invalid flag")
    end
    xlen = UInt16(data[11]) | UInt16(data[12]) << 8

    # +=================================+
    # |...XLEN bytes of "extra field"...| (more-->)
    # +=================================+
    unsafe_read(input, pointer(data, 13), xlen)
    bsize = UInt16(0) # size of block - 1
    pos = 13
    @inbounds while pos < 13 + xlen
        si1 = data[pos]
        si2 = data[pos+1]
        slen = UInt16(data[pos+2]) | UInt16(data[pos+3]) << 8
        if si1 == 0x42 || si2 == 0x43
            if slen != 2
                bgzferror("invalid subfield length")
            end
            bsize = unsafe_load(Ptr{UInt16}(pointer(data, pos+4)))
        end
        # skip this field
        pos += 4 + slen
    end
    if bsize == 0
        bgzferror("no block size")
    end
    block.compressed_data_position = 12 + xlen + 1
    block.compressed_data_size = bsize - xlen - 19

    # +=======================+---+---+---+---+---+---+---+---+
    # |...compressed blocks...|     CRC32     |     ISIZE     |
    # +=======================+---+---+---+---+---+---+---+---+
    unsafe_read(input, pointer(data, block.compressed_data_position), block.compressed_data_size + 8)

    pos = block.compressed_data_position + block.compressed_data_size
    crc = unsafe_load(Ptr{UInt32}(pointer(data, pos)))
    block.decompressed_data_size = unsafe_load(Ptr{Int32}(pointer(data, pos+4)))

    if eof(input) && !is_eof_block(block)
        bgzferror("no end-of-file marker (maybe a truncated file)")
    end

    return crc
end

# Return true iff the block is a end-of-file marker.
is_eof_block(block::Block) = view(block.block_data, 1:block_data_size(block)) == EOF_BLOCK

function decompress!(block::Block, crc::UInt32)
    # Decompress
    dstptr = pointer(block.decompressed_data)
    srcptr = pointer(block.block_data, block.compressed_data_position)
    unsafe_decompress!(Base.HasLength(), block.de_compressor,
                       dstptr, block.decompressed_data_size,
                       srcptr, block.compressed_data_size)

    # Calculate CRC
    actual_crc = unsafe_crc32(pointer(block.decompressed_data), block.decompressed_data_size)
    if actual_crc != crc
        bgzferror("CRC checksum does not match (maybe corrupted data)")
    end

    return nothing
end

function compress!(block::Block)
    # 18-byte header, so we compress to the 19th byte in the compressed block
    n_bytes = unsafe_compress!(block.de_compressor,
              pointer(block.block_data, 19), BGZF_MAX_BLOCK_SIZE - 8,
              pointer(block.decompressed_data), block.decompressed_data_size)

    block_size = n_bytes + 18 + 8 # 18 byte header + 8 byte tail

    # Write in the header including correct block size
    copyto!(block.block_data,
            # ID1   ID2    CM   FLG  |<--     MTIME    -->|   XFL    OS
            [0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff])
    copyto!(block.block_data, 11,
            #                     XLEN  S1  S2   SLEN   BSIZE (size of block - 1)
            reinterpret(UInt8, [0x0006, 0x4342, 0x0002, UInt16(block_size - 1)]))

    # Now write in the tail
    crc = unsafe_crc32(pointer(block.decompressed_data), block.decompressed_data_size)
    ptr = Ptr{UInt32}(pointer(block.decompressed_data, block_data_size(block) + 1))
    unsafe_store!(ptr, crc)
    unsafe_store!(ptr + 4, block.decompressed_data_size)
end

function write_compressed_block(io::IO, block::Block)
    nb = unsafe_write(io, pointer(block.block_data), block_data_size(block))
    nb == block_data_size(block) || error("Failed to write BGZF block")
    isa(io, Pipe) || (block.block_offset = position(io))
    block.decompressed_data_size = 0
    return nb
end

# BGZFStream
# ==========

# Stream type for the BGZF compression format.
mutable struct BGZFStream{T<:IO} <: IO
    # underlying IO stream
    io::T

    # read/write mode
    mode::UInt8

    # compressed & decompressed blocks
    blocks::Vector{Block}
    tasks::Vector{Task}
    crcs::Vector{UInt32}

    # current block index
    block_index::Int

    # whether stream is open
    isopen::Bool

    # callback function called when closing the stream
    onclose::Function
end


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

    mode_ = mode == "r" ? READ_MODE : WRITE_MODE
    N = Threads.nthreads()
    blocks = [Block(mode_) for i in 1:N]
    tasks = Vector{Task}(undef, N)
    crcs = Vector{UInt32}(undef, N)
    return BGZFStream(io, mode_, blocks, tasks, crcs, 1, true, io -> close(io))
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
        block = stream.blocks[stream.block_index]
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
        block = stream.blocks[stream.block_index]
        if !iszero(block.decompressed_data_size)
            write_blocks!(stream, stream.block_index)
        end
        write(stream.io, EOF_BLOCK)
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
    if inblock_offset ≥ block.decompressed_data_size
        throw(ArgumentError("too large in-block offset"))
    end
    block.block_offset = block_offset
    block.reading_position = inblock_offset + 1
    return
end

function Base.seek(stream::BGZFStream{T}, voffset::VirtualOffset) where {T<:Base.AbstractPipe}
    throw(ArgumentError("seek is not supported for a pipe stream"))
end

# Ensure buffered data (at least 1 byte) for reading and return the block index
# if available or 0 otherwise.
@inline function ensure_buffered_data(stream::BGZFStream)
    hasread = false # Make sure to only read blocks once
    while true
        while stream.block_index ≤ lastindex(stream.blocks)
            @inbounds block = stream.blocks[stream.block_index]
            if block.reading_position ≤ block.decompressed_data_size
                return stream.block_index
            end
            stream.block_index += 1
        end

        hasread && return 0
        hasread = true
        if !eof(stream.io)
            read_blocks!(stream)
            stream.block_index = 1
        end
    end
end

function read_blocks!(stream::BGZFStream)
    N = 0
    for i in 1:length(stream.blocks)
        if !eof(stream.io)
            stream.crcs[i] = read_bgzf_block!(stream.io, stream.blocks[i])
            N += 1
        end
    end
    for i in 1:N
        stream.tasks[i] = @spawn decompress!(stream.blocks[i], stream.crcs[i])
    end
    for i in 1:N
        wait(stream.tasks[i])
    end
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
    byte = block.decompressed_data[block.reading_position]
    block.reading_position += 1
    return byte
end


function Base.unsafe_read(stream::BGZFStream, p::Ptr{UInt8}, n::UInt)
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != READ_MODE
        throw(ArgumentError("stream is not readable"))
    end
    remaining = n
    while !iszero(remaining)
        i = ensure_buffered_data(stream)
        if i == 0
            throw(EOFError())
        end
        @inbounds block = stream.blocks[i]

        len = min(remaining, block.decompressed_data_size - block.reading_position + 1)
        src = pointer(block.decompressed_data, block.reading_position)
        memcpy(p, src, len)
        block.reading_position += len
        p += len
        remaining -= len
    end
end

"Write the first N blocks to stream's IO"
function write_blocks!(stream::BGZFStream, N::Int)
    for i in 1:N
        stream.tasks[i] = @spawn compress!(stream.blocks[i])
    end
    for i in 1:N
        wait(stream.tasks[i])
    end
    for i in 1:N
        write_compressed_block(stream.io, stream.blocks[i])
    end
    stream.block_index = 1
end

"Increment block_index or flush to io if full. Call this after all write ops"
function checkfull(stream::BGZFStream)
    block = stream.blocks[stream.block_index]
    if block.decompressed_data_size >= BGZF_SAFE_BLOCK_SIZE
        if stream.block_index < lastindex(stream.blocks)
            stream.block_index += 1
        else
            write_blocks!(stream, lastindex(stream.blocks))
        end
    end
end

function Base.write(stream::BGZFStream, byte::UInt8)
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != WRITE_MODE
        throw(ArgumentError("stream is not writable"))
    end

    block = stream.blocks[stream.block_index]
    block.decompressed_data_size += 1
    block.decompressed_data[block.decompressed_data_size] = byte
    checkfull(stream)
    return 1
end

function Base.unsafe_write(stream::BGZFStream, p::Ptr{UInt8}, n::UInt)
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != WRITE_MODE
        throw(ArgumentError("stream is not writable"))
    end
    remaining = n
    while !iszero(remaining)
        block = stream.blocks[stream.block_index]
        block_remaining_bytes = BGZF_SAFE_BLOCK_SIZE - block.decompressed_data_size
        len = min(remaining, block_remaining_bytes)
        dst = pointer(block.decompressed_data, block.decompressed_data_size + 1)
        memcpy(dst, p, len % UInt)
        p += len
        remaining -= len
        block.decompressed_data_size += len
        checkfull(stream)
    end
    return Int(n)
end

end # module
