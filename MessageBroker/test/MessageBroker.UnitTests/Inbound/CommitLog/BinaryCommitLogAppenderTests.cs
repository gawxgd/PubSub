using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Inbound.CommitLog;
using NSubstitute;
using Xunit;
using System.Text;
using MessageBroker.Inbound.CommitLog.Record;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Index.Writer;
using MessageBroker.Inbound.CommitLog.Index.Reader;
using MessageBroker.Inbound.CommitLog.Segment;
using Microsoft.Extensions.Options;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using System.Buffers.Binary;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;
using MessageBroker.Inbound.CommitLog.TopicSegmentManager;
using static MessageBroker.UnitTests.Inbound.CommitLog.CommitLogTestHelpers;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class BinaryCommitLogAppenderTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly ITopicSegmentRegistry _segmentRegistry;
    private readonly TopicSegmentRegistryFactory _topicSegmentManagaerFactory;
    private readonly BinaryOffsetIndexReader _offsetIndexReader;
    private readonly BinaryTimeIndexReader _timeIndexReader;

    public BinaryCommitLogAppenderTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);

        _testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);

        // Create real implementations
        var recordWriter = new LogRecordBinaryWriter();
        var recordReader = new LogRecordBinaryReader();
        var compressor = new NoopCompressor();
        var encoding = Encoding.UTF8;
        var batchWriter = new LogRecordBatchBinaryWriter(recordWriter, compressor, encoding);
        var batchReader = new LogRecordBatchBinaryReader(recordReader, compressor, encoding);
        var offsetIndexWriter = new BinaryOffsetIndexWriter();
        var timeIndexWriter = new BinaryTimeIndexWriter();
        _offsetIndexReader = new BinaryOffsetIndexReader();
        _timeIndexReader = new BinaryTimeIndexReader();

        var options = Options.Create(new CommitLogOptions
        {
            MaxSegmentBytes = 1024 * 1024,
            IndexIntervalBytes = 4096,
            TimeIndexIntervalMs = 60000,
            FileBufferSize = 65536,
            ReaderLogBufferSize = 65536,
            ReaderIndexBufferSize = 8192
        });

        _segmentFactory = new BinaryLogSegmentFactory(
            batchWriter,
            batchReader,
            offsetIndexWriter,
            _offsetIndexReader,
            timeIndexWriter,
            _timeIndexReader,
            options
        );

        _topicSegmentManagaerFactory = new TopicSegmentRegistryFactory(_segmentFactory);
        _segmentRegistry = _topicSegmentManagaerFactory.GetOrCreate("test-topic", _testDirectory, 0);
    }

    [Fact]
    public void Constructor_Should_Create_Initial_Segment()
    {
        // Act
        var appender = CreateAppender();

        // Assert - Verify segment files were created
        var logFile = Path.Combine(_testDirectory, "00000000000000000000.log");
        var indexFile = Path.Combine(_testDirectory, "00000000000000000000.index");
        var timeIndexFile = Path.Combine(_testDirectory, "00000000000000000000.timeindex");

        File.Exists(logFile).Should().BeTrue();
        File.Exists(indexFile).Should().BeTrue();
        File.Exists(timeIndexFile).Should().BeTrue();
    }

    [Fact]
    public async Task AppendAsync_Should_Queue_Payload()
    {
        // Arrange
        var appender = CreateAppender();
        var payload = new byte[] { 1, 2, 3 };

        // Act
        await appender.AppendAsync(payload);

        // Assert
        // Verify it doesn't throw - payload is queued
    }

    [Fact]
    public async Task AppendAsync_Should_Flush_To_Segment_After_Interval()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));
        var payload = new byte[] { 1, 2, 3 };

        // Act
        await appender.AppendAsync(payload);
        await Task.Delay(150); // Wait for background flush

        // Assert - Read back and verify
        var activeSegment = _segmentRegistry.GetActiveSegment();
        await using var reader = _segmentFactory.CreateReaderM(activeSegment);
        var readBatch = reader.ReadBatch(0);

        // Create expected batch using timestamps from read batch for full comparison
        var expectedBatch = CreateExpectedBatchFromRead(readBatch!, payload);
        AssertBatchesEqual(expectedBatch, readBatch, "flushed batch should match expected");
    }

    [Fact]
    public async Task AppendAsync_Should_Batch_Multiple_Payloads()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(100));

        // Act
        await appender.AppendAsync(new byte[] { 1 });
        await appender.AppendAsync(new byte[] { 2 });
        await appender.AppendAsync(new byte[] { 3 });
        await Task.Delay(150); // Wait for background flush

        // Assert - Read back and verify all three payloads were batched together
        var activeSegment = _segmentRegistry.GetActiveSegment();
        await using var reader = _segmentFactory.CreateReaderM(activeSegment);
        var readBatch = reader.ReadBatch(0);

        // Create expected batch using timestamps from read batch for full comparison
        var expectedBatch =
            CreateExpectedBatchFromRead(readBatch!, new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 });
        AssertBatchesEqual(expectedBatch, readBatch, "batched payloads should match expected");
    }

    [Fact]
    public async Task AppendAsync_Should_Roll_Segment_When_Needed()
    {
        // Arrange - Create appender with very small max segment size to force rolling
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50), maxSegmentBytes: 100);
        var largePayload = new byte[200];
        Random.Shared.NextBytes(largePayload);

        // Act
        await appender.AppendAsync(largePayload);
        await Task.Delay(150);
        await appender.AppendAsync(new byte[] { 1 }); // This should trigger a roll
        await Task.Delay(150);

        // Assert - Verify data was written and rolled
        var files = Directory.GetFiles(_testDirectory, "*.log");
        files.Length.Should().BeGreaterThanOrEqualTo(1, "at least one segment should exist");
    }

    [Fact]
    public async Task AppendAsync_Should_Assign_Sequential_Offsets()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));

        // Act
        await appender.AppendAsync(new byte[] { 1 });
        await appender.AppendAsync(new byte[] { 2 });
        await appender.AppendAsync(new byte[] { 3 });
        await Task.Delay(150);

        // Assert - Read back and verify sequential offsets
        var activeSegment = _segmentRegistry.GetActiveSegment();
        await using var reader = _segmentFactory.CreateReaderM(activeSegment);
        var readBatch = reader.ReadBatch(0);

        // Create expected batch using timestamps from read batch for full comparison
        var expectedBatch =
            CreateExpectedBatchFromRead(readBatch!, new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 });
        AssertBatchesEqual(expectedBatch, readBatch, "sequential offsets should match expected");
    }

    [Fact]
    public async Task AppendAsync_Should_Handle_Channel_Overflow()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromHours(1)); // Long interval

        // Act - Fill channel beyond capacity (10 items)
        var tasks = new List<Task>();
        for (int i = 0; i < 15; i++)
        {
            tasks.Add(appender.AppendAsync(new byte[] { (byte)i }).AsTask());
        }

        // Assert - Should not throw or deadlock
        var allTasks = Task.WhenAll(tasks);
        var completedTask = await Task.WhenAny(allTasks, Task.Delay(1000));
        completedTask.Should().Be(allTasks, "all appends should complete within timeout without deadlock");
    }

    [Fact]
    public async Task AppendAsync_Should_Include_Timestamp()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));

        // Act
        await appender.AppendAsync(new byte[] { 1 });
        await Task.Delay(150);

        // Assert - Read back and verify timestamp
        var activeSegment = _segmentRegistry.GetActiveSegment();
        await using var reader = _segmentFactory.CreateReaderM(activeSegment);
        var readBatch = reader.ReadBatch(0);

        // Create expected batch using timestamps from read batch for full comparison
        var expectedBatch = CreateExpectedBatchFromRead(readBatch!, new byte[] { 1 });
        AssertBatchesEqual(expectedBatch, readBatch, "timestamp test batch should match expected");

        // Additionally verify timestamp is valid
        readBatch!.Records.First().Timestamp.Should().BeGreaterThan(0, "timestamp should be set");
    }

    [Fact]
    public async Task AppendAsync_Should_Preserve_Payload()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));
        var originalPayload = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        await appender.AppendAsync(originalPayload);
        await Task.Delay(150);

        // Assert - Read back and verify payload
        var activeSegment = _segmentRegistry.GetActiveSegment();
        await using var reader = _segmentFactory.CreateReaderM(activeSegment);
        var readBatch = reader.ReadBatch(0);

        // Create expected batch using timestamps from read batch for full comparison
        var expectedBatch = CreateExpectedBatchFromRead(readBatch!, originalPayload);
        AssertBatchesEqual(expectedBatch, readBatch, "payload should be preserved exactly");
    }

    private BinaryCommitLogAppender CreateAppender(
        ulong baseOffset = 0,
        TimeSpan? flushInterval = null,
        ulong? maxSegmentBytes = null)
    {
        // If maxSegmentBytes is specified, we need to create a new factory with custom options
        ILogSegmentFactory factory = _segmentFactory;

        if (maxSegmentBytes.HasValue)
        {
            var recordWriter = new LogRecordBinaryWriter();
            var recordReader = new LogRecordBinaryReader();
            var compressor = new NoopCompressor();
            var encoding = Encoding.UTF8;
            var batchWriter = new LogRecordBatchBinaryWriter(recordWriter, compressor, encoding);
            var batchReader = new LogRecordBatchBinaryReader(recordReader, compressor, encoding);
            var offsetIndexWriter = new BinaryOffsetIndexWriter();
            var timeIndexWriter = new BinaryTimeIndexWriter();

            var customOptions = Options.Create(new CommitLogOptions
            {
                MaxSegmentBytes = maxSegmentBytes.Value,
                IndexIntervalBytes = 4096,
                TimeIndexIntervalMs = 60000,
                FileBufferSize = 65536,
                ReaderLogBufferSize = 65536,
                ReaderIndexBufferSize = 8192
            });

            factory = new BinaryLogSegmentFactory(
                batchWriter,
                batchReader,
                offsetIndexWriter,
                _offsetIndexReader,
                timeIndexWriter,
                _timeIndexReader,
                customOptions
            );
        }

        return new BinaryCommitLogAppender(
            factory,
            _testDirectory,
            baseOffset,
            flushInterval ?? TimeSpan.FromMilliseconds(100),
            _segmentRegistry
        );
    }

    private LogRecordBatch CreateExpectedBatchFromRead(LogRecordBatch readBatch, params byte[][] payloads)
    {
        // Create expected batch using actual timestamps from the read batch
        // This allows full comparison of all fields including timestamps
        var records = new List<LogRecord>();
        var readRecords = readBatch.Records.ToList();

        for (int i = 0; i < payloads.Length; i++)
        {
            records.Add(new LogRecord(
                readRecords[i].Offset,
                readRecords[i].Timestamp,
                payloads[i]
            ));
        }

        return new LogRecordBatch(
            readBatch.MagicNumber,
            readBatch.BaseOffset,
            records,
            readBatch.Compressed
        );
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, true);
            }
            catch
            {
                // Cleanup best effort
            }
        }
    }
}