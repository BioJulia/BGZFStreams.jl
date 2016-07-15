using BGZFStreams
using Base.Test
using Compat

@testset "VirtualOffset" begin
    voff = VirtualOffset(0, 0)
    @test voff == VirtualOffset(0, 0)
    @test voff != VirtualOffset(0, 1)
    @test voff != VirtualOffset(1, 0)
    @test BGZFStreams.offsets(voff) == (0, 0)
    voff += 1
    @test BGZFStreams.offsets(voff) == (0, 1)
end

@testset "BGZFStream" begin
    filename = Pkg.dir("BGZFStreams", "test", "bar.bgz")
    stream = BGZFStream(filename, "r")
    @test read(stream, UInt8) === UInt8('b')
    @test read(stream, UInt8) === UInt8('a')
    @test read(stream, UInt8) === UInt8('r')
    @test eof(stream)
    close(stream)
    @test_throws ArgumentError read(stream, UInt8)

    stream = BGZFStream(filename, "r")
    @test virtualoffset(stream) === VirtualOffset(0, 0)
    read(stream, UInt8)
    read(stream, UInt8)
    @test virtualoffset(stream) === VirtualOffset(0, 2)
    seek(stream, VirtualOffset(0, 1))
    @test read(stream, UInt8) === UInt8('a')
    @test read(stream, UInt8) === UInt8('r')
    seekstart(stream)
    @test read(stream, UInt8) === UInt8('b')
    @test read(stream, UInt8) === UInt8('a')
    @test read(stream, UInt8) === UInt8('r')
    close(stream)

    # Empty data.
    empty_block = copy(BGZFStreams.EOF_BLOCK)
    stream = BGZFStream(IOBuffer(empty_block))
    @test_throws EOFError read(stream, UInt8)
    @test_throws EOFError read(stream, UInt8)
    stream = BGZFStream(IOBuffer(vcat(empty_block, empty_block)))
    @test_throws EOFError read(stream, UInt8)
    @test_throws EOFError read(stream, UInt8)
    stream = BGZFStream(IOBuffer(empty_block))
    @test isempty(read(stream))
    @test isempty(read(stream))
    stream = BGZFStream(IOBuffer(vcat(empty_block, empty_block)))
    @test isempty(read(stream))
    @test isempty(read(stream))

    filename = tempname()
    try
        stream = BGZFStream(filename, "w")
        @test write(stream, 'b') === 1
        @test write(stream, 'a') === 1
        @test write(stream, 'r') === 1
        @test write(stream, "baz") === 3
        @test eof(stream)
        close(stream)
    catch
        rethrow()
    finally
        if isfile(filename)
            rm(filename)
        end
    end

    buffer = IOBuffer()
    stream = BGZFStream(buffer, "w")
    write(stream, "foo")
    close(stream)
    @test !isopen(buffer)
    @test_throws ArgumentError write(stream, 0x01)

    # Round trip
    for n in [0, 1, 2, 5, 10, 50, 100, 10_000, 100_000, 1_000_000]
        data = rand(0x00:0xf0, n)

        # bulk read/write
        buffer = IOBuffer()
        stream = BGZFStream(buffer, "w")
        # HACK: do not close the buffer after the stream is closed
        stream.onclose = io -> nothing
        write(stream, data)
        close(stream)
        seekstart(buffer)
        stream = BGZFStream(buffer)
        @test data == read(stream)
        close(stream)

        # read/write byte by byte
        buffer = IOBuffer()
        stream = BGZFStream(buffer, "w")
        # HACK: do not close the buffer after the stream is closed
        stream.onclose = io -> nothing
        for x in data
            write(stream, x)
        end
        close(stream)
        seekstart(buffer)
        stream = BGZFStream(buffer)
        data′ = UInt8[]
        while !eof(stream)
            push!(data′, read(stream, UInt8))
        end
        @test data == data′
        close(stream)
    end
end

@testset "Tabix" begin
    bedfile = Pkg.dir("BGZFStreams", "test", "knownGene.part.bed.gz")
    indexfile = bedfile * ".tbi"

    @testset "load" begin
        index = Tabix(indexfile)
        @test isa(index, Tabix)
        @test index.format === Int32(0x10000)
        @test index.columns === (1, 2, 3)
        @test index.meta === '#'
        @test index.skip === 0
        @test index.names == ["chr1", "chr2", "chr3"]
        @test length(index.indexes) == 3
    end

    @testset "chunks" begin
        stream = BGZFStream(bedfile)
        index = Tabix(indexfile)

        # Expected values were counted using the tabix tool as follows:
        #   $ tabix test/knownGene.part.bed.gz chr1:5,000,000-10,000,000 | wc -l
        #   $     64
        for (seqname, interval, expected) in [
                ("chr1", 5_000_000:10_000_000, 64),
                ("chr1", 6_000_000:10_000_000, 54),
                ("chr2", 5_000_000:10_000_000, 81),
                ("chr3", 9_000_000:10_000_000, 15),
                ("chr3", 9_800_000:10_000_000,  3),
                ("chr3", 9_900_000:10_000_000,  0)]
            n = 0
            for chunk in overlapchunks(index, seqname, interval)
                seek(stream, chunk)
                while virtualoffset(stream) in chunk
                    line = readline(stream)
                    values = split(chomp(line), '\t')
                    nm = values[index.columns[1]]
                    @test nm == seqname
                    int = parse(Int, values[index.columns[2]]):parse(Int, values[index.columns[3]])-1
                    if !isempty(intersect(int, interval))
                        n += 1
                    elseif first(int) > last(interval)
                        break
                    end
                end
            end
            @test n == expected
        end
    end
end
