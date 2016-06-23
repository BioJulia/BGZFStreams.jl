# Virtual Offset
# ==============

bitstype 64 VirtualOffset

function Base.convert(::Type{VirtualOffset}, x::UInt64)
    return reinterpret(VirtualOffset, x)
end

function Base.convert(::Type{UInt64}, x::VirtualOffset)
    return reinterpret(UInt64, x)
end

"""
    VirtualOffset(file_offset::Integer, in_block_offset::Integer)

Create a virtual file offset from `file_offset` and `in_block_offset`.

`file_offset` is an offset pointing to the beggining position of a BGZF block in
a BGZF file and `in_block_offset` is an offset pointing to the begining position
of a binary data within a uncompressed BGZF block. These values are zero-based
and their valid ranges are [0, 1 << 48) and [0, 1 << 16), respectively.
"""
function VirtualOffset(file_offset::Integer, in_block_offset::Integer)
    if !(0 ≤ file_offset < (1 << 48))
        throw(ArgumentError("block file offset must be in [0, $(1 << 48))"))
    elseif !(0 ≤ in_block_offset < (1 << 16))
        throw(ArgumentError("in-block offset must be in [0, $(1 << 16))"))
    end
    return convert(VirtualOffset, (UInt64(file_offset) << 16) | UInt64(in_block_offset))
end

function Base.isless(x::VirtualOffset, y::VirtualOffset)
    return isless(convert(UInt64, x), convert(UInt64, y))
end

# NOTE: This doesn't check the valid range of virtual offset.
function Base.:+(voffset::VirtualOffset, x::Integer)
    return convert(VirtualOffset, convert(UInt64, voffset) + UInt64(x))
end

function offsets(voffset::VirtualOffset)
    x = convert(UInt64, voffset)
    return x >> 16, x & 0xffff
end

function file_offset(voffset::VirtualOffset)
    return convert(UInt64, voffset) >> 16
end

function block_offset(voffset::VirtualOffset)
    return convert(UInt64, voffset) & 0xffff
end

function Base.show(io::IO, voffset::VirtualOffset)
    x, y = offsets(voffset)
    print(io, summary(voffset), "(", x, ", ", y, ")")
end
