# BGZFStream
# ==========

type BGZFStream{T<:IO} <: IO
    # underlying IO stream
    io::T

    # zstream object
    zstream::Libz.ZStream

    # space for the compressed block
    compressed_block::Vector{UInt8}

    # space for the decompressed block
    decompressed_block::Vector{UInt8}

    # virtual offset
    offset::VirtualOffset

    # read/write mode
    mode::UInt8

    # number of available bytes in the decompressed block
    size::Int

    # whether stream is open
    isopen::Bool
end

# BGZF blocks are no larger than 64 KiB before and after compression.
const BGZF_MAX_BLOCK_SIZE = 64 * 1024

# Read mode:  inflate and read a BGZF file
# Write mode: deflate and write a BGZF file
const READ_MODE  = 0x00
const WRITE_MODE = 0x01

"""
    BGZFStream(io::IO[, mode::AbstractString="r"])
    BGZFStream(filename::AbstractString[, mode::AbstractString="r"])

Create an IO stream for the BGZF compression format.

The first argument is either an `IO` object or a filename. If `mode` is `"r"`
(read) the BGZF stream will be in read mode and decompress the underlying BGZF
blocks while reading. In read mode, `BGZFStream` supports the `seek` operation
using a virtual file offset (see `VirtualOffset`). If `mode` is `"w"` (write)
or `"a"` (append) the BGZF stream will be in write mode and compress written
data to BGZF blocks.
"""

function BGZFStream(io::IO, mode::AbstractString="r")
    if mode == "r"
        zstream = Libz.init_inflate_zstream(true)[]
    elseif mode == "w" || mode == "a"
        if mode == "w"
            seekstart(io)
        end
        zstream = Libz.init_deflate_stream(
            true, 6, 8,
            Libz.Z_DEFAULT_STRATEGY)[]
    else
        throw(ArgumentError("invalid mode: '", mode, "'"))
    end

    return BGZFStream(
        io,
        zstream,
        Vector{UInt8}(BGZF_MAX_BLOCK_SIZE),
        Vector{UInt8}(BGZF_MAX_BLOCK_SIZE),
        VirtualOffset(position(io), 0),
        mode == "r" ? READ_MODE : WRITE_MODE,
        mode == "r" ? 0 : BGZF_MAX_BLOCK_SIZE,
        true)
end

function BGZFStream(filename::AbstractString, mode::AbstractString="r")
    return BGZFStream(open(filename, mode), mode)
end

"""
    virtualoffset(stream::BGZFStream)

Return the current virtual file offset of `stream`.
"""
function virtualoffset(stream::BGZFStream)
    return stream.offset
end

function Base.isopen(stream::BGZFStream)
    return stream.isopen
end

function Base.close(stream::BGZFStream)
    if stream.mode == WRITE_MODE
        if has_buffered_data(stream)
            write_block(stream)
        end
        write(stream.io, EOF_BLOCK)
    end
    close(stream.io)
    end_zstream(stream)
    stream.isopen = false
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
        if has_buffered_data(stream)
            return false
        elseif eof(stream.io)
            return true
        end
        read_block(stream)
        return !has_buffered_data(stream)
    else
        return true
    end
end

function Base.seek(stream::BGZFStream, voffset::VirtualOffset)
    if stream.mode == WRITE_MODE
        throw(ArgumentError("BGZFStream in write mode is not seekable"))
    end
    seek(stream.io, file_offset(voffset))
    read_block(stream)
    stream.offset = voffset
    return
end

function Base.read(stream::BGZFStream, ::Type{UInt8})
    if stream.mode != READ_MODE
        throw(ArgumentError("BGZFStream is not readable"))
    elseif !has_buffered_data(stream)
        read_block(stream)
        if !has_buffered_data(stream)
            throw(EOFError())
        end
    end
    stream.offset += 1
    byte = stream.decompressed_block[block_offset(stream.offset)]
    return byte
end

function Base.write(stream::BGZFStream, byte::UInt8)
    if stream.mode != WRITE_MODE
        throw(ArgumentError("BGZFStream is not writable"))
    elseif block_offset(stream.offset) == stream.size
        write_block(stream)
    end
    stream.offset += 1
    stream.decompressed_block[block_offset(stream.offset)] = byte
    return 1
end


# Internal functions
# ------------------

# Return true iff the stream has buffered data in the decompressed block.
function has_buffered_data(stream)
    if stream.mode == READ_MODE
        return block_offset(stream.offset) < stream.size
    else
        return block_offset(stream.offset) > 0
    end
end

immutable BGZFDataError <: Exception
    message::ASCIIString
end

# Throw a BGZFDataError exception with the given error message.
function bgzferror(message::AbstractString="malformed BGZF data")
    throw(BGZFDataError(message))
end

# Read a compressed block and inflate it.
function read_block(stream)
    file_offset = position(stream.io)
    bsize = read_bgzf_block(stream)

    zstream = stream.zstream
    zstream.next_in = pointer(stream.compressed_block)
    zstream.avail_in = bsize
    zstream.next_out = pointer(stream.decompressed_block)
    zstream.avail_out = BGZF_MAX_BLOCK_SIZE

    ret = ccall(
        (:inflate, Libz._zlib),
        Cint,
        (Ref{Libz.ZStream}, Cint),
        zstream, Libz.Z_FINISH)
    if ret != Libz.Z_STREAM_END
        if ret == Libz.Z_OK
            error("block size may exceed BGZF_MAX_BLOCK_SIZE")
        else
            error("failed to decompress a BGZF block (zlib error $(ret))")
        end
    end
    stream.offset = VirtualOffset(file_offset, 0)
    stream.size = BGZF_MAX_BLOCK_SIZE - zstream.avail_out

    reset_zstream(stream)
end

# Read a compressed BGZF block and return the size.
function read_bgzf_block(stream)
    source = stream.io
    block = stream.compressed_block

    # +---+---+---+---+---+---+---+---+---+---+
    # |ID1|ID2|CM |FLG|     MTIME     |XFL|OS | (more-->)
    # +---+---+---+---+---+---+---+---+---+---+
    n = readbytes!(source, block, 10)
    if n != 10
        bgzferror()
    end
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
    n = readbytes!(source, sub(block, 11:endof(block)), 2)
    if n != 2
        bgzferror()
    end
    xlen = UInt16(block[11]) | UInt16(block[12]) << 8
    n = readbytes!(source, sub(block, 13:endof(block)), xlen)
    if n != xlen
        bgzferror()
    end
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
    #readbytes!(source, sub(block, (13 + xlen):endof(block)), bsize - (12 + xlen))
    n = readbytes!(source, sub(block, (13 + xlen):endof(block)), bsize - 1 - xlen - 19 + 8)
    if n != bsize - 1 - xlen - 19 + 8
        bgzferror()
    end

    if eof(source) && !is_eof_block(block)
        bgzferror("no end-of-file marker (maybe a truncated file)")
    end

    return bsize
end

function write_block(stream)
    zstream = stream.zstream
    zstream.next_in = pointer(stream.decompressed_block)
    zstream.avail_in = block_offset(stream.offset)
    zstream.next_out = pointer(stream.compressed_block, 9)
    zstream.avail_out = BGZF_MAX_BLOCK_SIZE - 8

    ret = ccall(
        (:deflate, Libz._zlib),
        Cint,
        (Ref{Libz.ZStream}, Cint),
        zstream, Libz.Z_FINISH)
    if ret != Libz.Z_STREAM_END
        if ret == Libz.Z_OK
            error("block size may exceed BGZF_MAX_BLOCK_SIZE")
        else
            error("failed to compress a BGZF block (zlib error $(ret))")
        end
    end

    blocksize = (BGZF_MAX_BLOCK_SIZE - 8) - zstream.avail_out + 8
    fix_header!(stream.compressed_block, blocksize)
    write(stream.io, sub(stream.compressed_block, 1:blocksize))
    stream.offset = VirtualOffset(position(stream.io), 0)

    reset_zstream(stream)
end

function fix_header!(block, blocksize)
    copy!(block,
          # ID1   ID2    CM   FLG  |<--     MTIME    -->|   XFL    OS
          [0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff])
    copy!(block, 11,
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
    for i in 1:endof(EOF_BLOCK)
        if block[i] != EOF_BLOCK[i]
            return false
        end
    end
    return true
end

# Reset the zstream.
function reset_zstream(stream)
    if stream.mode == READ_MODE
        ret = ccall(
            (:inflateReset, Libz._zlib),
            Cint,
            (Ref{Libz.ZStream},),
            stream.zstream)
    else
        ret = ccall(
            (:deflateReset, Libz._zlib),
            Cint,
            (Ref{Libz.ZStream},),
            stream.zstream)
    end
    if ret != Libz.Z_OK
        error("failed to reset zlib stream")
    end
end

# End the zstream.
function end_zstream(stream)
    if stream.mode == READ_MODE
        ret = ccall(
            (:inflateEnd, Libz._zlib),
            Cint,
            (Ref{Libz.ZStream},),
            stream.zstream)
    else
        ret = ccall(
            (:deflateEnd, Libz._zlib),
            Cint,
            (Ref{Libz.ZStream},),
            stream.zstream)
    end
    if ret != Libz.Z_OK
        error("failed to end zlib stream")
    end
end
