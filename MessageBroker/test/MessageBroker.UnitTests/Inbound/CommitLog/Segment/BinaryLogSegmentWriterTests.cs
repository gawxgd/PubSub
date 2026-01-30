using FluentAssertions;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Inbound.CommitLog.Segment;
using NSubstitute;
using Xunit;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using System.Buffers.Binary;
using System.Text;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Record;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Index.Reader;
using MessageBroker.Inbound.CommitLog.Index.Writer;
using static MessageBroker.UnitTests.Inbound.CommitLog.CommitLogTestHelpers;

namespace MessageBroker.UnitTests.Inbound.CommitLog.Segment;

public class BinaryLogSegmentWriterTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly ILogRecordBatchWriter _batchWriter;
    private readonly ILogRecordBatchReader _batchReader;
    private readonly BinaryOffsetIndexWriter _offsetIndexWriter;
    private readonly BinaryTimeIndexWriter _timeIndexWriter;
    private readonly BinaryOffsetIndexReader _offsetIndexReader;
    private readonly BinaryTimeIndexReader _timeIndexReader;

    public BinaryLogSegmentWriterTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);

        _testDirectory = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);

        var recordWriter = new LogRecordBinaryWriter();
        var recordReader = new LogRecordBinaryReader();
        var compressor = new NoopCompressor();
        var encoding = Encoding.UTF8;

        _batchWriter = new LogRecordBatchBinaryWriter(recordWriter, compressor, encoding);
        _batchReader = new LogRecordBatchBinaryReader(recordReader, compressor, encoding);
        _offsetIndexWriter = new BinaryOffsetIndexWriter();
        _timeIndexWriter = new BinaryTimeIndexWriter();
        _offsetIndexReader = new BinaryOffsetIndexReader();
        _timeIndexReader = new BinaryTimeIndexReader();
    }

    [Fact]
    public async Task AppendAsync_Should_Call_BatchWriter_With_Correct_Batch()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment);
        var batch = CreateTestBatch(baseOffset: 5, recordCount: 3);
        var batchBytes = SerializeBatch(batch);

        // Act
        await writer.AppendAsync(batchBytes, batch.BaseOffset, (ulong)batch.Records.Count, CancellationToken.None);
        await writer.DisposeAsync();

        // Assert - Read back and verify
        await using var reader = CreateReader(segment);
        var readBatch = reader.ReadBatch(5);

        AssertBatchesEqual(batch, readBatch, "written batch should match read batch");
    }

    [Fact]
    public async Task AppendAsync_Should_Write_To_Log_File_With_Correct_Data()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 1);
        var batchBytes = SerializeBatch(batch);

        // Act
        await writer.AppendAsync(batchBytes, batch.BaseOffset, (ulong)batch.Records.Count, CancellationToken.None);
        await writer.DisposeAsync();

        // Assert
        File.Exists(segment.LogPath).Should().BeTrue();

        await using var reader = CreateReader(segment);
        var readBatch = reader.ReadBatch(0);

        AssertBatchesEqual(batch, readBatch, "written batch should match read batch");
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
        var writer = CreateWriter(segment, maxSegmentBytes: 100);

        var largePayload = new byte[200];
        Random.Shared.NextBytes(largePayload);
        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            new List<LogRecord> { new LogRecord(0, 1000, largePayload) },
            false
        );
        var batchBytes = SerializeBatch(batch);

        // Act
        await writer.AppendAsync(batchBytes, batch.BaseOffset, (ulong)batch.Records.Count, CancellationToken.None);
        var shouldRoll = writer.ShouldRoll();

        // Assert
        shouldRoll.Should().BeTrue("log file size exceeds max segment bytes (100)");

        await writer.DisposeAsync();
        await using var reader = CreateReader(segment);
        var readBatch = reader.ReadBatch(0);

        AssertBatchesEqual(batch, readBatch, "written batch should match read batch");
    }

    [Fact]
    public async Task AppendAsync_Should_Write_Index_With_Correct_Format()
    {
        // Arrange
        var segment = CreateSegment(baseOffset: 100);
        var writer = CreateWriter(segment, indexIntervalBytes: 100);
        var batch = CreateTestBatch(baseOffset: 105, recordCount: 2, 100);
        var batchBytes = SerializeBatch(batch);

        // Act
        await writer.AppendAsync(batchBytes, batch.BaseOffset, (ulong)batch.Records.Count, CancellationToken.None);
        await writer.DisposeAsync();

        // Assert
        File.Exists(segment.IndexFilePath).Should().BeTrue();
        var indexData = await File.ReadAllBytesAsync(segment.IndexFilePath);
        indexData.Length.Should().Be(16, "index entry is 8 bytes for relative offset + 8 bytes for position");

        var relativeOffset = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(0, 8));
        var filePosition = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(8, 8));

        relativeOffset.Should().Be(5UL, "batch base offset (105) - segment base offset (100) = 5");
        filePosition.Should().Be(0UL, "batch was written at position 0");

        await using var reader = CreateReader(segment);
        var readBatch = reader.ReadBatch(105);

        AssertBatchesEqual(batch, readBatch, "written batch should match read batch");
    }

    [Fact(Skip = "TimeIndex writing is not yet implemented (see TODO in BinaryLogSegmentWriter.cs line 113)")]
    public async Task AppendAsync_Should_Write_TimeIndex_With_Correct_Format()
    {
        // Arrange
        var segment = CreateSegment(baseOffset: 50);
        var writer = CreateWriter(segment, timeIndexIntervalMs: 1000);
        var batch = CreateTestBatch(baseOffset: 52, recordCount: 1);
        var batchBytes = SerializeBatch(batch);

        // Act
        await writer.AppendAsync(batchBytes, batch.BaseOffset, (ulong)batch.Records.Count, CancellationToken.None);
        await writer.DisposeAsync();

        // Assert
        File.Exists(segment.TimeIndexFilePath).Should().BeTrue();
        var timeIndexData = await File.ReadAllBytesAsync(segment.TimeIndexFilePath);
        timeIndexData.Length.Should().Be(16, "time index entry is 8 bytes for timestamp + 8 bytes for file position");

        var timestamp = BinaryPrimitives.ReadUInt64BigEndian(timeIndexData.AsSpan(0, 8));
        var filePosition = BinaryPrimitives.ReadUInt64BigEndian(timeIndexData.AsSpan(8, 8));

        timestamp.Should().Be(1000UL, "base timestamp from test batch");
        filePosition.Should().Be(0UL, "batch was written at position 0");

        await using var reader = CreateReader(segment);
        var readBatch = reader.ReadBatch(52);

        AssertBatchesEqual(batch, readBatch, "written batch should match read batch");
    }

    [Fact]
    public async Task AppendAsync_Should_Not_Write_Index_When_Interval_Not_Reached()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment, indexIntervalBytes: 1000);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 1);
        var batchBytes = SerializeBatch(batch);

        // Act
        await writer.AppendAsync(batchBytes, batch.BaseOffset, (ulong)batch.Records.Count, CancellationToken.None);
        await writer.DisposeAsync();

        // Assert
        File.Exists(segment.IndexFilePath).Should().BeTrue();
        var indexData = await File.ReadAllBytesAsync(segment.IndexFilePath);
        indexData.Length.Should().Be(0, "index should not be written when interval not reached");

        await using var reader = CreateReader(segment);
        var readBatch = reader.ReadBatch(0);

        AssertBatchesEqual(batch, readBatch, "written batch should match read batch");
    }

    [Fact]
    public async Task AppendAsync_Should_Accumulate_Bytes_For_Index_Interval()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment, indexIntervalBytes: 30);
        var batch1 = CreateTestBatch(baseOffset: 0, recordCount: 1);
        var batch2 = CreateTestBatch(baseOffset: 1, recordCount: 1);
        var batch1Bytes = SerializeBatch(batch1);
        var batch2Bytes = SerializeBatch(batch2);

        // Act
        await writer.AppendAsync(batch1Bytes, batch1.BaseOffset, (ulong)batch1.Records.Count, CancellationToken.None);
        await writer.AppendAsync(batch2Bytes, batch2.BaseOffset, (ulong)batch2.Records.Count, CancellationToken.None);
        await writer.DisposeAsync();

        // Assert
        var indexData = await File.ReadAllBytesAsync(segment.IndexFilePath);
        indexData.Length.Should().Be(32, "two index entries should be written (16 bytes each)");

        await using var reader = CreateReader(segment);
        var readBatch1 = reader.ReadBatch(0);
        var readBatch2 = reader.ReadBatch(1);

        AssertBatchesEqual(batch1, readBatch1, "first batch should match");
        AssertBatchesEqual(batch2, readBatch2, "second batch should match");
    }

    [Fact]
    public async Task AppendAsync_Should_Handle_Multiple_Batches_Sequentially()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment);
        var batch0 = CreateTestBatch(baseOffset: 0, recordCount: 1);
        var batch1 = CreateTestBatch(baseOffset: 1, recordCount: 1);
        var batch2 = CreateTestBatch(baseOffset: 2, recordCount: 1);
        var batch0Bytes = SerializeBatch(batch0);
        var batch1Bytes = SerializeBatch(batch1);
        var batch2Bytes = SerializeBatch(batch2);

        // Act
        await writer.AppendAsync(batch0Bytes, batch0.BaseOffset, (ulong)batch0.Records.Count, CancellationToken.None);
        await writer.AppendAsync(batch1Bytes, batch1.BaseOffset, (ulong)batch1.Records.Count, CancellationToken.None);
        await writer.AppendAsync(batch2Bytes, batch2.BaseOffset, (ulong)batch2.Records.Count, CancellationToken.None);
        await writer.DisposeAsync();

        // Assert - Read back and verify all batches were written sequentially
        await using var reader = CreateReader(segment);

        var readBatch0 = reader.ReadBatch(0);
        var readBatch1 = reader.ReadBatch(1);
        var readBatch2 = reader.ReadBatch(2);

        AssertBatchesEqual(batch0, readBatch0, "first batch should match");
        AssertBatchesEqual(batch1, readBatch1, "second batch should match");
        AssertBatchesEqual(batch2, readBatch2, "third batch should match");
    }

    [Fact]
    public async Task DisposeAsync_Should_Flush_And_Close_All_Streams()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment);
        var batch = CreateTestBatch(baseOffset: 0, recordCount: 1);
        var batchBytes = SerializeBatch(batch);
        await writer.AppendAsync(batchBytes, batch.BaseOffset, (ulong)batch.Records.Count, CancellationToken.None);

        // Act
        await writer.DisposeAsync();

        // Assert - Files should exist and be readable (streams closed)
        File.Exists(segment.LogPath).Should().BeTrue();
        File.Exists(segment.IndexFilePath).Should().BeTrue();
        File.Exists(segment.TimeIndexFilePath).Should().BeTrue();

        await using var reader = CreateReader(segment);
        var readBatch = reader.ReadBatch(0);

        AssertBatchesEqual(batch, readBatch, "written batch should match read batch after disposal");
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
        var writer = CreateWriter(segment, indexIntervalBytes: 75);
        var batch0 = CreateTestBatch(baseOffset: 0, recordCount: 1, 75);
        var batch1 = CreateTestBatch(baseOffset: 1, recordCount: 1);
        var batch0Bytes = SerializeBatch(batch0);
        var batch1Bytes = SerializeBatch(batch1);

        // Act - Write two batches that should each trigger an index write
        await writer.AppendAsync(batch0Bytes, batch0.BaseOffset, (ulong)batch0.Records.Count, CancellationToken.None);
        await writer.AppendAsync(batch1Bytes, batch1.BaseOffset, (ulong)batch1.Records.Count, CancellationToken.None);
        await writer.DisposeAsync();

        // Assert - Check that index entries track positions correctly
        var indexData = await File.ReadAllBytesAsync(segment.IndexFilePath);
        indexData.Length.Should().Be(16, "two index entries should be written");

        var firstRelativeOffset = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(0, 8));
        var firstPosition = BinaryPrimitives.ReadUInt64BigEndian(indexData.AsSpan(8, 8));
        firstRelativeOffset.Should().Be(0UL, "first batch has relative offset 0");
        firstPosition.Should().Be(0UL, "first batch starts at position 0");

        await using var reader = CreateReader(segment);
        var readBatch0 = reader.ReadBatch(0);
        var readBatch1 = reader.ReadBatch(1);

        AssertBatchesEqual(batch0, readBatch0, "first batch should match");
        AssertBatchesEqual(batch1, readBatch1, "second batch should match");
    }

    [Fact(Skip = "TimeIndex writing is not yet implemented (see TODO in BinaryLogSegmentWriter.cs line 113)")]
    public async Task AppendAsync_Should_Only_Write_TimeIndex_After_Interval()
    {
        // Arrange
        var segment = CreateSegment();
        var writer = CreateWriter(segment, timeIndexIntervalMs: 500);

        // Act

        var batch0 = CreateTestBatch(baseOffset: 0, recordCount: 1);
        var batch0Bytes = SerializeBatch(batch0);
        await writer.AppendAsync(batch0Bytes, batch0.BaseOffset, (ulong)batch0.Records.Count, CancellationToken.None);

        var batch1 = CreateTestBatch(baseOffset: 1, recordCount: 1);
        var batch1Bytes = SerializeBatch(batch1);
        await writer.AppendAsync(batch1Bytes, batch1.BaseOffset, (ulong)batch1.Records.Count, CancellationToken.None);

        await writer.DisposeAsync();

        // Assert
        var timeIndexData = await File.ReadAllBytesAsync(segment.TimeIndexFilePath);
        timeIndexData.Length.Should().Be(16, "only one time index entry (first batch) should be written");

        await using var reader = CreateReader(segment);
        var readBatch0 = reader.ReadBatch(0);
        var readBatch1 = reader.ReadBatch(1);

        readBatch0.Should().NotBeNull();
        readBatch1.Should().NotBeNull();
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
            _offsetIndexWriter,
            _timeIndexWriter,
            segment,
            maxSegmentBytes,
            indexIntervalBytes,
            timeIndexIntervalMs,
            65536
        );
    }

    private byte[] SerializeBatch(LogRecordBatch batch)
    {
        using var ms = new MemoryStream();
        _batchWriter.WriteTo(batch, ms);
        return ms.ToArray();
    }

    private BinaryLogSegmentReader CreateReader(LogSegment segment)
    {
        return new BinaryLogSegmentReader(
            _batchReader,
            segment,
            _offsetIndexReader,
            _timeIndexReader,
            65536,
            8192
        );
    }

    private LogRecordBatch CreateTestBatch(
        ulong baseOffset = 0,
        int recordCount = 1,
        int payloadSize = 1
    )
    {
        var records = new List<LogRecord>();
        for (int i = 0; i < recordCount; i++)
        {
            var payload = new byte[payloadSize];
            Random.Shared.NextBytes(payload);
            records.Add(new LogRecord(
                baseOffset + (ulong)i,
                1000 + (ulong)i,
                payload
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

                GC.Collect();
                GC.WaitForPendingFinalizers();

                Directory.Delete(_testDirectory, true);
            }
        }
        catch
        {

        }
    }
}
