using BGZFStreams
using Base.Test
using Compat

voff = VirtualOffset(0, 0)
@test voff == VirtualOffset(0, 0)
@test voff != VirtualOffset(0, 1)
@test voff != VirtualOffset(1, 0)
@test BGZFStreams.file_offset(voff) == 0
@test BGZFStreams.block_offset(voff) == 0
voff += 1
@test BGZFStreams.file_offset(voff) == 0
@test BGZFStreams.block_offset(voff) == 1

filename = Pkg.dir("BGZFStreams", "test", "bar.bgz")
stream = BGZFStream(filename, "r")
@test read(stream, UInt8) === UInt8('b')
@test read(stream, UInt8) === UInt8('a')
@test read(stream, UInt8) === UInt8('r')
@test eof(stream)
close(stream)

stream = BGZFStream(filename, "r")
@test virtualoffset(stream) === VirtualOffset(0, 0)
read(stream, UInt8)
read(stream, UInt8)
@test virtualoffset(stream) === VirtualOffset(0, 2)
seek(stream, VirtualOffset(0, 1))
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

# Round trip
for n in [0, 1, 2, 5, 10, 50, 100, 10_000, 100_000, 1_000_000]
    buffer = IOBuffer()
    stream = BGZFStream(buffer, "w")
    # HACK: do not close the buffer after the stream is closed
    stream.onclose = io -> nothing
    data = rand(0x00:0xf0, n)
    write(stream, data)
    close(stream)

    seekstart(buffer)
    stream = BGZFStream(buffer)
    data′ = read(stream)
    @test data′ == data
    close(stream)
end
