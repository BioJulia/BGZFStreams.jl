# Virtual Offset
# ==============

primitive type VirtualOffset 64 end

function Base.convert(::Type{VirtualOffset}, x::UInt64)
    return reinterpret(VirtualOffset, x)
end

function Base.convert(::Type{UInt64}, x::VirtualOffset)
    return reinterpret(UInt64, x)
end

"""
    VirtualOffset(block_offset::Integer, inblock_offset::Integer)

Create a virtual file offset from `block_offset` and `inblock_offset`.

`block_offset` is an offset pointing to the beggining position of a BGZF block
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
    return convert(VirtualOffset, (UInt64(block_offset) << 16) | UInt64(inblock_offset))
end

function Base.isless(x::VirtualOffset, y::VirtualOffset)
    return isless(convert(UInt64, x), convert(UInt64, y))
end

# NOTE: This doesn't check the valid range of virtual offset.
function Base.:+(voffset::VirtualOffset, x::Integer)
    return convert(VirtualOffset, convert(UInt64, voffset) + UInt64(x))
end

function Base.getindex(voffset::VirtualOffset, i::Integer)
    return offsets(voffset)[i]
end

function offsets(voffset::VirtualOffset)
    x = convert(UInt64, voffset)
    return x >> 16, x & 0xffff
end

function Base.show(io::IO, voffset::VirtualOffset)
    block_offset, inblock_offset = offsets(voffset)
    print(io, summary(voffset), "(", block_offset, ", ", inblock_offset, ")")
end

function Base.read(io::IO, ::Type{VirtualOffset})
    return convert(VirtualOffset, read(io, UInt64))
end

function Base.write(io::IO, voffset::VirtualOffset)
    return write(io, convert(UInt64, voffset))
end
