# Tabix
# =====
#
# Generic index for tab-delimited files.
#
# Li, Heng. "Tabix: fast retrieval of sequence features from generic
# TAB-delimited files." Bioinformatics 27.5 (2011): 718-719.
# Specification: http://samtools.github.io/hts-specs/tabix.pdf

# Binning index
typealias BinIndex Dict{UInt32,Vector{Chunk}}

# Linear index
typealias LinearIndex Vector{VirtualOffset}

type Tabix
    format::Int32
    columns::NTuple{3,Int}
    meta::Char
    skip::Int
    names::Vector{String}
    indexes::Vector{Tuple{BinIndex,LinearIndex}}
    n_no_coor::Nullable{UInt64}
end

function Base.show(io::IO, index::Tabix)
    println(io, summary(index), ":")
    println(io, "  format: ", format2str(index.format))
    println(io, "  columns: ", index.columns)
    println(io, "  meta char: '", index.meta, "'")
    println(io, "  skip lines: ", index.skip)
      print(io, "  names: ", index.names)
end

"""
    Tabix(filename::AbstractString)
    Tabix(input::IO)
    read(filename::AbstractString, ::Type{Tabix})
    read(input::IO, ::Type{Tabix})

Load a Tabix index from a file.
"""
function Tabix(filename::AbstractString)
    return read(filename, Tabix)
end

function Tabix(input::IO)
    return read(input, Tabix)
end

function Base.read(filename::AbstractString, ::Type{Tabix})
    return open(input -> read(input, Tabix), filename)
end

function Base.read(input_::IO, ::Type{Tabix})
    input = BGZFStream(input_)

    # magic
    T = read(input, UInt8)
    B = read(input, UInt8)
    I = read(input, UInt8)
    x = read(input, UInt8)
    if T != UInt8('T') || B != UInt8('B') || I != UInt8('I') || x != 0x01
        error("invalid tabix magic bytes")
    end

    n_refs = read(input, Int32)
    format = read(input, Int32)
    col_seq = read(input, Int32)
    col_beg = read(input, Int32)
    col_end = read(input, Int32)
    meta = read(input, Int32)
    skip = read(input, Int32)

    l_nm = read(input, Int32)
    data = read(input, UInt8, l_nm)
    names = split(String(data), '\0', keep=false)

    indexes = Tuple{BinIndex,LinearIndex}[]
    for n in 1:n_refs
        binindex = BinIndex()
        n_bins = read(input, Int32)
        for i in 1:n_bins
            bin = read(input, UInt32)
            n_chunks = read(input, Int32)
            chunks = Chunk[]
            for j in 1:n_chunks
                cnk_beg = read(input, UInt64)
                cnk_end = read(input, UInt64)
                push!(chunks, Chunk(cnk_beg, cnk_end))
            end
            binindex[bin] = chunks
        end

        n_intvs = read(input, Int32)
        linindex = LinearIndex()
        for i in 1:n_intvs
            ioff = read(input, UInt64)
            push!(linindex, ioff)
        end

        push!(indexes, (binindex, linindex))
    end

    if !eof(input)
        n_no_coor = Nullable(read(input, UInt64))
    else
        n_no_coor = Nullable{UInt64}()
    end

    return Tabix(
        format,
        (col_seq, col_beg, col_end),
        meta,
        skip,
        names,
        indexes,
        n_no_coor)
end

#=
for chunk in overlapchunks(index, seqname, interval)
    seek(stream, chunk)
    while virtualoffset(stream) in chunk
        line = chomp(readline(stream))
        values = split(line, '\t')
        if <no overlap>
            break
        end
        # do something
    end
end
=#

function Base.seek(stream::BGZFStream, chunk::Chunk)
    seek(stream, chunk.start)
end

# The BED rule is half-closed-half-open and 0-based like Python.
function is_bed_rule(format)
    return format & 0x10000 != 0
end

function format2str(format)
    if format == 1
        return "SAM"
    elseif format == 2
        return "VCF"
    else
        if is_bed_rule(format)
            return "generic (BED rule)"
        else
            return "generic"
        end
    end
end

const LinearWindowSize = 16 * 1024

"""
    overlapchunks(index::Tabix, seqname::String, interval::UnitRange)
    overlapchunks(index::Tabix, seqid::Integer, interval::UnitRange)

Return chunks overlapping the range specified by `seqname` (or `seqid`) and `interval`;
`seqid` and `interval` must be 1-based index and inclusive.
"""
function overlapchunks(index::Tabix, seqid::Integer, interval::UnitRange)
    if !(1 ≤ seqid ≤ endof(index.indexes))
        throw(ArgumentError("sequence id $(seqid) is out of range"))
    end

    if isempty(interval)
        return Chunk[]
    end

    binindex, linindex = index.indexes[seqid]
    bins = reg2bins(first(interval), last(interval))
    offset = linindex[cld(first(interval), LinearWindowSize)]
    ret = Chunk[]
    for bin in bins
        if haskey(binindex, bin)
            for chunk in binindex[bin]
                if chunk.stop > offset
                    push!(ret, chunk)
                end
            end
        end
    end

    sort!(ret)
    reduce!(ret)

    return ret
end

function overlapchunks(index::Tabix, seqname::AbstractString, interval::UnitRange)
    seqid = findfirst(index.names, seqname)
    if seqid == 0
        throw(ArgumentError("sequence name $(seqname) is not included in the index"))
    end
    return overlapchunks(index, seqid, interval)
end

# Merge chunks to minimize the number of seek operations.
function reduce!(chunks)
    @assert issorted(chunks)
    i = 1
    while i < endof(chunks)
        chunk = chunks[i]
        next = chunks[i+1]
        if chunk.stop < next.start
            # neither overlapping nor adjacent
            i += 1
            continue
        end
        chunks[i] = Chunk(chunk.start, max(chunk.stop, next.stop))
        deleteat!(chunks, i + 1)
    end
    return chunks
end

# Calculate bins overlapping a region [from, to] (one-based).
function reg2bins(from, to)
    bins = UInt32[]
    bin_start = 0
    for scale in 29:-3:14
        for k in ((from - 1) >> scale):((to - 1) >> scale)
            push!(bins, bin_start + k)
        end
        bin_start = 8 * bin_start + 1
    end
    return bins
end
