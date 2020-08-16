const BLOCK_HEADER = [
  # ID1   ID2   CM    FLG   |<--     MTIME    -->|
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
  # XFL   OS    |  XLEN  |  S1    S2    |  SLEN  |
    0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00
]

# end-of-file marker block (used for detecting unintended file truncation)
const EOF_BLOCK = vcat(BLOCK_HEADER, [
  # |  BSIZE |  |  DATA  |  |        CRC32       |
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
  # |        ISIZE       |
    0x00, 0x00, 0x00, 0x00
])

# BGZF blocks are no larger than 64 KiB before and after compression.
const MAX_BLOCK_SIZE = UInt(64 * 1024)

# BGZF_MAX_BLOCK_SIZE minus "margin for safety"
# NOTE: Data block will become slightly larger after deflation when bytes are
# randomly distributed.
const SAFE_BLOCK_SIZE = UInt(MAX_BLOCK_SIZE - 256)

struct Block
    compress_pos::UInt32
    compress_len::UInt32
    decompress_pos::UInt32
    decompress_len::UInt32
    blocklen::UInt32
    crc32::UInt32
end

function check_eof_block(block::Block, data::Vector{UInt8}, pos::Integer)
    if view(data, pos:pos+block.blocklen-1) != EOF_BLOCK
        bgzferror("No EOF block. Truncated file?")
    end
end

function decompress!(decompressor::Decompressor, block::Block, outdata::Vector{UInt8}, indata::Vector{UInt8})
    unsafe_decompress!(Base.HasLength(), decompressor,
                       pointer(outdata, block.decompress_pos), block.decompress_len,
                       pointer(indata, block.compress_pos), block.compress_len)
    
    actual_crc = unsafe_crc32(pointer(outdata, block.decompress_pos), block.decompress_len)
    actual_crc == block.crc32 || bgzferror("CRC32 checksum does not match")
end

"Make a new block at pos in data, returning block size"
function copy_block!(data::Vector{UInt8}, block::Block, pos::Integer)
    # Header: 18 bytes of header
    unsafe_copyto!(data, pos, BLOCK_HEADER, 1, 16)
    unsafe_store!(Ptr{UInt16}(pointer(data, pos + 16)), UInt16(block.blocklen - 1))
    pos += 18

    # Meat
    unsafe_copyto!(data, pos, data, block.compress_pos, block.compress_len)
    pos += block.compress_len

    # Tail
    unsafe_store!(Ptr{UInt32}(pointer(data, pos)), block.crc32)
    unsafe_store!(Ptr{UInt32}(pointer(data, pos + 4)), block.decompress_len)
    pos += 8
    return block.blocklen
end

"Compress ith block with uncompressed data length len"
function compress_block!(compressor::Compressor, indata::Vector{UInt8}, outdata::Vector{UInt8},
                   i::Integer, len::Integer)
    # Make room for 18 byte header
    compress_pos = (i-1) * MAX_BLOCK_SIZE + 18 + 1
    decompress_pos = (i-1) * SAFE_BLOCK_SIZE + 1
    compress_len = unsafe_compress!(compressor,
                   pointer(outdata, compress_pos), MAX_BLOCK_SIZE - 26,
                   pointer(indata, decompress_pos), len)

    crc = unsafe_crc32(pointer(indata, decompress_pos), len)
    return Block(compress_pos, compress_len, decompress_pos, len, compress_len+26, crc)
end

# Read a whole BGZF block from the pointer, into block
function index!(data::Vector{UInt8}, pos::Integer, lastpos::Integer, decpos::Integer)
    # +---+---+---+---+---+---+---+---+---+---+---+---+
    # |ID1|ID2|CM |FLG|     MTIME     |XFL|OS | XLEN  | (more-->)
    # +---+---+---+---+---+---+---+---+---+---+---+---+
    (pos + 12 - 1) > lastpos && bgzferror("Too small input")
    @inbounds begin
        if !((data[pos] == 0x1f) & (data[pos+1] == 0x8b))
            bgzferror("invalid gzip identifier")
        elseif !(data[pos+2] == 0x08)
            bgzferror("invalid compression method")
        elseif !(data[pos+3] == 0x04)
            bgzferror("invalid flag")
        end
    end
    xlen = UInt16(data[pos+10]) | UInt16(data[pos+11]) << 8
    pos += 12

    # +=================================+
    # |...XLEN bytes of "extra field"...| (more-->)
    # +=================================+
    (pos + xlen - 1) > lastpos && bgzferror("Too small input")
    stop = pos + xlen
    bsize = UInt16(0) # size of block - 1
    @inbounds while pos < stop
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
    compress_pos = pos
    compress_len = (bsize + 1) - (12 + xlen + 8)
    pos += compress_len
    
    # +=======================+---+---+---+---+---+---+---+---+
    # |...compressed blocks...|     CRC32     |     ISIZE     |
    # +=======================+---+---+---+---+---+---+---+---+
    (pos + 8 - 1) > lastpos && bgzferror("Too small input")
    crc = unsafe_load(Ptr{UInt32}(pointer(data, pos)))
    isize = unsafe_load(Ptr{UInt32}(pointer(data, pos + 4)))

    return Block(compress_pos, compress_len, decpos, isize, bsize + 1, crc)
end
