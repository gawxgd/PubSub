using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Inbound.CommitLog.Segment;
using NSubstitute;
using Xunit;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using System.Buffers.Binary;

namespace MessageBroker.UnitTests.Inbound.CommitLog.Segment;

public class BinaryLogSegmentWriterTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly ILogRecordBatchWriter _batchWriter;

    public BinaryLogSegmentWriterTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);
        
        _testDirectory = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
        _batchWriter = Substitute.For<ILogRecordBatchWriter>();
    }

    [Fact]
    public async Task AppendAsync_Should_Call_BatchWriter_With_Correct_Batch()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment);
        var batch = CreateTestBatch(baseOffset: 5, recordCount: 3);
        
        LogRecordBatch? capturedBatch = null;
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call => capturedBatch = call.Arg<LogRecordBatch>());

        // Act
        await writer.AppendAsync(batch);
        await writer.DisposeAsync();

        // Assert
        _batchWriter.Received(1).WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>());
        capturedBatch.Should().BeSameAs(batch);
    }

    [Fact]
    public async Task AppendAsync_Should_Write_To_Log_File_With_Correct_Data()
    {
        // Arrange
        var segment = CreateSegment();
        var expectedData = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD, 0xEE };
        
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call =>
            {
                var stream = call.Arg<Stream>();
                stream.Write(expectedData);
            });
        
        var writer = CreateWriter(segment);
        var batch = CreateTestBatch();

        // Act
        await writer.AppendAsync(batch);
        await writer.DisposeAsync();

        // Assert
        File.Exists(segment.LogPath).Should().BeTrue();
        var actualData = await File.ReadAllBytesAsync(segment.LogPath);
        actualData.Should().BeEquivalentTo(expectedData);
    }

    [Fact]
    public void ShouldRoll_Should_Return_False_Initially()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment, maxSegmentBytes: 1024 * 1024);

        // Act
        var shouldRoll = writer.ShouldRoll();

        // Assert
        shouldRoll.Should().BeFalse("log file is empty and under the limit");
    }

    [Fact]
    public async Task ShouldRoll_Should_Return_True_When_Log_Exceeds_MaxSegmentBytes()
    {
        // Arrange
        var segment = CreateSegment();
        var dataSize = 150;
        
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call =>
            {
                var stream = call.Arg<Stream>();
                stream.Write(new byte[dataSize]);
            });
        
        var writer = CreateWriter(segment, maxSegmentBytes: 100);
        var batch = CreateTestBatch();

        // Act
        await writer.AppendAsync(batch);
        var shouldRoll = writer.ShouldRoll();

        // Assert
        shouldRoll.Should().BeTrue($"log file size ({dataSize}) exceeds max segment bytes (100)");
        
        await writer.DisposeAsync();
    }

    [Fact]
    public async Task AppendAsync_Should_Write_Index_With_Correct_Format()
    {
        // Arrange
        var segment = CreateSegment(baseOffset: 100);
        var bytesWritten = 150;
        
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call =>
            {
                var stream = call.Arg<Stream>();
                stream.Write(new byte[bytesWritten]);
            });
        
        var writer = CreateWriter(segment, indexIntervalBytes: 100);
        var batch = CreateTestBatch(baseOffset: 105, recordCount: 2);

        // Act
        await writer.AppendAsync(batch);
        await writer.DisposeAsync();

        // Assert
        File.Exists(segment.IndexFilePath).Should().BeTrue();
        var indexData = await File.ReadAllBytesAsync(segment.IndexFilePath);
        indexData.Length.Should().Be(16, "index entry is 8 bytes for relative offset + 8 bytes for position");
        
        // Verify index format: relative offset (8 bytes) + file position (8 bytes)
        var relativeOffset = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(0, 8));
        var filePosition = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(8, 8));
        
        relativeOffset.Should().Be(5UL, "batch base offset (105) - segment base offset (100) = 5");
        filePosition.Should().Be(0UL, "batch was written at position 0");
    }

    [Fact]
    public async Task AppendAsync_Should_Write_TimeIndex_With_Correct_Format()
    {
        // Arrange
        var segment = CreateSegment(baseOffset: 50);
    
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call => call.Arg<Stream>().Write(new byte[10]));
    
        // Use a positive interval - first write will always happen because _lastTimeIndexTimestamp starts at 0
        var writer = CreateWriter(segment, timeIndexIntervalMs: 1000);
        var batch = CreateTestBatch(baseOffset: 52, recordCount: 1);

        // Act
        await writer.AppendAsync(batch);
        await writer.DisposeAsync();

        // Assert
        File.Exists(segment.TimeIndexFilePath).Should().BeTrue();
        var timeIndexData = await File.ReadAllBytesAsync(segment.TimeIndexFilePath);
        timeIndexData.Length.Should().Be(16, "time index entry is 8 bytes for timestamp + 8 bytes for relative offset");
    
        var timestamp = BinaryPrimitives.ReadUInt64BigEndian(timeIndexData.AsSpan(0, 8));
        var relativeOffset = BinaryPrimitives.ReadUInt64BigEndian(timeIndexData.AsSpan(8, 8));
    
        timestamp.Should().Be(1000UL, "base timestamp from test batch");
        relativeOffset.Should().Be(2UL, "batch base offset (52) - segment base offset (50) = 2");
    }

    [Fact]
    public async Task AppendAsync_Should_Not_Write_Index_When_Interval_Not_Reached()
    {
        // Arrange
        var segment = CreateSegment();
        
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call => call.Arg<Stream>().Write(new byte[50])); // Less than indexIntervalBytes
        
        var writer = CreateWriter(segment, indexIntervalBytes: 1000);
        var batch = CreateTestBatch();

        // Act
        await writer.AppendAsync(batch);
        await writer.DisposeAsync();

        // Assert
        File.Exists(segment.IndexFilePath).Should().BeTrue();
        var indexData = await File.ReadAllBytesAsync(segment.IndexFilePath);
        indexData.Length.Should().Be(0, "index should not be written when interval not reached");
    }

    [Fact]
    public async Task AppendAsync_Should_Accumulate_Bytes_For_Index_Interval()
    {
        // Arrange
        var segment = CreateSegment();
        var bytesPerBatch = 60;
        
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call => call.Arg<Stream>().Write(new byte[bytesPerBatch]));
        
        var writer = CreateWriter(segment, indexIntervalBytes: 100);

        // Act
        await writer.AppendAsync(CreateTestBatch(baseOffset: 0)); // 60 bytes
        await writer.AppendAsync(CreateTestBatch(baseOffset: 1)); // 120 bytes total - should write index
        await writer.DisposeAsync();

        // Assert
        var indexData = await File.ReadAllBytesAsync(segment.IndexFilePath);
        indexData.Length.Should().Be(16, "index should be written after accumulating >= 100 bytes");
    }

    [Fact]
    public async Task AppendAsync_Should_Handle_Multiple_Batches_Sequentially()
    {
        // Arrange
        var segment = CreateSegment();
        var callCount = 0;
        
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call =>
            {
                callCount++;
                call.Arg<Stream>().Write(new byte[] { (byte)callCount });
            });
        
        var writer = CreateWriter(segment);

        // Act
        await writer.AppendAsync(CreateTestBatch(baseOffset: 0));
        await writer.AppendAsync(CreateTestBatch(baseOffset: 1));
        await writer.AppendAsync(CreateTestBatch(baseOffset: 2));
        await writer.DisposeAsync();

        // Assert
        _batchWriter.Received(3).WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>());
        
        var logData = await File.ReadAllBytesAsync(segment.LogPath);
        logData.Should().Equal(new byte[] { 1, 2, 3 }, "batches should be written sequentially");
    }

    [Fact]
    public async Task DisposeAsync_Should_Flush_And_Close_All_Streams()
    {
        // Arrange
        var segment = CreateSegment();
        
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call => call.Arg<Stream>().Write(new byte[] { 0xFF }));
        
        var writer = CreateWriter(segment);
        await writer.AppendAsync(CreateTestBatch());

        // Act
        await writer.DisposeAsync();

        // Assert - Files should exist and be readable (streams closed)
        File.Exists(segment.LogPath).Should().BeTrue();
        File.Exists(segment.IndexFilePath).Should().BeTrue();
        File.Exists(segment.TimeIndexFilePath).Should().BeTrue();
        
        // Should be able to read files after disposal
        var logData = await File.ReadAllBytesAsync(segment.LogPath);
        logData.Should().NotBeEmpty();
    }

    [Fact]
    public async Task DisposeAsync_Should_Be_Idempotent()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment);

        // Act
        await writer.DisposeAsync();
        var act = async () => await writer.DisposeAsync();

        // Assert
        await act.Should().NotThrowAsync("disposing multiple times should be safe");
    }

    [Fact]
    public void Constructor_Should_Create_Nested_Directories()
    {
        // Arrange
        var nestedDir = Path.Combine(_testDirectory, "level1", "level2", "level3");
        var segment = new LogSegment(
            Path.Combine(nestedDir, "test.log"),
            Path.Combine(nestedDir, "test.index"),
            Path.Combine(nestedDir, "test.timeindex"),
            0,
            0
        );

        // Act
        var writer = CreateWriter(segment);

        // Assert
        Directory.Exists(nestedDir).Should().BeTrue("nested directories should be created");
        File.Exists(segment.LogPath).Should().BeTrue("log file should be created");
        File.Exists(segment.IndexFilePath).Should().BeTrue("index file should be created");
        File.Exists(segment.TimeIndexFilePath).Should().BeTrue("timeindex file should be created");
        
        writer.DisposeAsync().AsTask().Wait();
    }

    [Fact]
    public async Task AppendAsync_Should_Track_File_Position_Correctly()
    {
        // Arrange
        var segment = CreateSegment();
        var firstBatchSize = 100;
        var secondBatchSize = 80; // Changed to exceed threshold after accumulation
    
        var writeCount = 0;
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call =>
            {
                var size = writeCount == 0 ? firstBatchSize : secondBatchSize;
                call.Arg<Stream>().Write(new byte[size]);
                writeCount++;
            });
    
        var writer = CreateWriter(segment, indexIntervalBytes: 75);

        // Act
        await writer.AppendAsync(CreateTestBatch(baseOffset: 0)); // 100 bytes - writes index, resets counter to 0
        await writer.AppendAsync(CreateTestBatch(baseOffset: 1)); // 80 bytes - exceeds 75, writes second index
        await writer.DisposeAsync();

        // Assert
        var indexData = await File.ReadAllBytesAsync(segment.IndexFilePath);
        indexData.Length.Should().Be(32, "two index entries should be written (100 >= 75, then 80 >= 75)");
    
        // First index entry (written at position 0 for batch at offset 0)
        var firstRelativeOffset = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(0, 8));
        var firstPosition = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(8, 8));
        firstRelativeOffset.Should().Be(0UL, "first batch has relative offset 0");
        firstPosition.Should().Be(0UL, "first batch starts at position 0");
    
        // Second index entry (written at position 100 for batch at offset 1)
        var secondRelativeOffset = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(16, 8));
        var secondPosition = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(24, 8));
        secondRelativeOffset.Should().Be(1UL, "second batch has relative offset 1");
        secondPosition.Should().Be(100UL, "second batch starts at position 100");
    }

    [Fact]
    public async Task AppendAsync_Should_Only_Write_TimeIndex_After_Interval()
    {
        // Arrange
        var segment = CreateSegment();
        
        _batchWriter.When(x => x.WriteTo(Arg.Any<LogRecordBatch>(), Arg.Any<Stream>()))
            .Do(call => call.Arg<Stream>().Write(new byte[10]));
        
        var writer = CreateWriter(segment, timeIndexIntervalMs: 500);

        // Act
        // First batch at timestamp 1000
        await writer.AppendAsync(CreateTestBatch(baseOffset: 0, recordCount: 1)); // Timestamp 1000
        
        // Second batch at timestamp 1001 (delta = 1ms < 500ms) - should not write time index
        await writer.AppendAsync(CreateTestBatch(baseOffset: 1, recordCount: 1)); // Timestamp 1001
        
        await writer.DisposeAsync();

        // Assert
        var timeIndexData = await File.ReadAllBytesAsync(segment.TimeIndexFilePath);
        timeIndexData.Length.Should().Be(16, "only one time index entry (first batch) should be written");
    }

    private LogSegment CreateSegment(ulong baseOffset = 0)
    {
        return new LogSegment(
            Path.Combine(_testDirectory, $"{baseOffset:D20}.log"),
            Path.Combine(_testDirectory, $"{baseOffset:D20}.index"),
            Path.Combine(_testDirectory, $"{baseOffset:D20}.timeindex"),
            baseOffset,
            baseOffset
        );
    }

    private BinaryLogSegmentWriter CreateWriter(
        LogSegment segment,
        ulong maxSegmentBytes = 1024 * 1024,
        uint indexIntervalBytes = 4096,
        uint timeIndexIntervalMs = 60000)
    {
        return new BinaryLogSegmentWriter(
            _batchWriter,
            segment,
            maxSegmentBytes,
            indexIntervalBytes,
            timeIndexIntervalMs,
            65536
        );
    }

    private LogRecordBatch CreateTestBatch(ulong baseOffset = 0, int recordCount = 1)
    {
        var records = new List<LogRecord>();
        for (int i = 0; i < recordCount; i++)
        {
            records.Add(new LogRecord(
                baseOffset + (ulong)i, 
                1000 + (ulong)i, 
                new byte[] { (byte)i }
            ));
        }
        
        return new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            baseOffset,
            records,
            false
        );
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDirectory))
            {
                // Ensure all files are closed before deletion
                GC.Collect();
                GC.WaitForPendingFinalizers();
                
                Directory.Delete(_testDirectory, true);
            }
        }
        catch
        {
            // Cleanup best effort - test environment will clean up eventually
        }
    }
}
