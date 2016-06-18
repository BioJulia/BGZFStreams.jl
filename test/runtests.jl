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

filename = tempname()
try
    stream = BGZFStream(filename, "w")
    @test write(stream, 'b') === 1
    @test write(stream, 'a') === 1
    @test write(stream, 'r') === 1
    @test eof(stream)
    close(stream)
catch
    rethrow()
finally
    if isfile(filename)
        rm(filename)
    end
end

for n in [1, 100, 10000]
    try
        filename = tempname()
        stream = BGZFStream(filename, "w")
        data = rand(0x00:0xf0, n)
        write(stream, data)
        close(stream)

        stream = BGZFStream(filename)
        data′ = read(stream, 2n)
        @test data′ == data
        close(stream)
    catch
        rethrow()
    finally
        if isfile(filename)
            rm(filename)
        end
    end
end
