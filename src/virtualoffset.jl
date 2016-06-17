# Virtual Offset
# ==============

bitstype 64 VirtualOffset

Base.convert(::Type{VirtualOffset}, x::UInt64) = reinterpret(VirtualOffset, x)
Base.convert(::Type{UInt64}, x::VirtualOffset) = reinterpret(UInt64, x)

function VirtualOffset(coffset::Integer, uoffset::Integer)
    if !(0 ≤ coffset < (1 << 48))
        throw(ArgumentError("block file offset must be in [0, $(1 << 48))"))
    elseif !(0 ≤ uoffset < (1 << 16))
        throw(ArgumentError("in-block offset must be in [0, $(1 << 16))"))
    end
    return convert(VirtualOffset, (UInt64(coffset) << 16) | UInt64(uoffset))
end

function Base.(:(==))(x::VirtualOffset, y::VirtualOffset)
    return convert(UInt64, x) == convert(UInt64, y)
end

function Base.isless(x::VirtualOffset, y::VirtualOffset)
    return isless(convert(UInt64, x), convert(UInt64, y))
end

# NOTE: This doesn't check the valid range of virtual offset.
function Base.(:+)(voffset::VirtualOffset, x::Integer)
    return convert(VirtualOffset, convert(UInt64, voffset) + UInt64(x))
end

function offsets(voffset::VirtualOffset)
    x = convert(UInt64, voffset)
    return x >> 16, x & ((UInt64(1) << 16) - 1)
end

function file_offset(voffset::VirtualOffset)
    x = convert(UInt64, voffset)
    return x >> 16
end

function block_offset(voffset::VirtualOffset)
    x = convert(UInt64, voffset)
    return x & ((UInt64(1) << 16) - 1)
end

function Base.show(io::IO, voffset::VirtualOffset)
    x, y = offsets(voffset)
    print(io, summary(voffset), "(", x, ", ", y, ")")
end
