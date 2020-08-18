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

function Base.show(io::IO, x::VirtualOffset)
    v, o = offsets(x)
    print(io, summary(x), '(', v, ", ", o, ')')
end

Base.read(io::IO, ::Type{VirtualOffset}) = VirtualOffset(read(io, UInt64))
Base.write(io::IO, voffset::VirtualOffset) = write(io, voffset.x)
