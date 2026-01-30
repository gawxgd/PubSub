using System.Collections.Concurrent;
using System.Text;
using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Compressor;
using MessageBroker.Domain.Port.CommitLog.Record;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;
using MessageBroker.Inbound.CommitLog;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Record;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class BinaryCommitLogAppenderThreadSafetyTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly ILogSegmentWriter _segmentWriter;
    private readonly ITopicSegmentRegistry _segmentRegistry;
    private readonly ConcurrentBag<LogRecordBatch> _capturedBatches;
    private readonly ILogRecordBatchReader _batchReader;

    public BinaryCommitLogAppenderThreadSafetyTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);

        _testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);

        _segmentWriter = Substitute.For<ILogSegmentWriter>();
        _segmentFactory = Substitute.For<ILogSegmentFactory>();
        _segmentRegistry = Substitute.For<ITopicSegmentRegistry>();
        _capturedBatches = new ConcurrentBag<LogRecordBatch>();
        
        var recordReader = new LogRecordBinaryReader();
        var compressor = new NoopCompressor();
        var encoding = Encoding.UTF8;
        _batchReader = new LogRecordBatchBinaryReader(recordReader, compressor, encoding);

        var testSegment = new LogSegment(
            Path.Combine(_testDirectory, "00000000000000000000.log"),
            Path.Combine(_testDirectory, "00000000000000000000.index"),
            Path.Combine(_testDirectory, "00000000000000000000.timeindex"),
            0,
            0
        );

        _segmentFactory.CreateLogSegment(Arg.Any<string>(), Arg.Any<ulong>())
            .Returns(testSegment);
        _segmentFactory.CreateWriter(Arg.Any<LogSegment>())
            .Returns(_segmentWriter);

        _segmentWriter.ShouldRoll().Returns(false);

        _segmentWriter.AppendAsync(Arg.Any<byte[]>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var batchBytes = call.Arg<byte[]>();
                using var ms = new MemoryStream(batchBytes);
                var batch = _batchReader.ReadBatch(ms);
                _capturedBatches.Add(batch);
                return ValueTask.CompletedTask;
            });
    }

    [Fact]
    public async Task AppendAsync_Should_Handle_Multiple_Concurrent_Appends()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(200));
        var taskCount = 100;
        var tasks = new List<Task>();

        // Act - Multiple threads appending concurrently
        for (int i = 0; i < taskCount; i++)
        {
            var batchBytes = CreateBatchBytes(new byte[] { (byte)i });
            tasks.Add(Task.Run(async () => await appender.AppendAsync(batchBytes)));
        }

        await Task.WhenAll(tasks);
        await Task.Delay(300);

        // Assert
        var totalRecords = _capturedBatches.Sum(b => b.Records.Count);
        totalRecords.Should().Be(taskCount, "all appends should be processed");
    }

    [Fact]
    public async Task AppendAsync_Should_Maintain_Offset_Uniqueness_Under_Concurrency()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(200));
        var taskCount = 50;
        var tasks = new List<Task>();

        // Act
        for (int i = 0; i < taskCount; i++)
        {
            var batchBytes = CreateBatchBytes(new byte[] { (byte)i });
            tasks.Add(Task.Run(async () => await appender.AppendAsync(batchBytes)));
        }

        await Task.WhenAll(tasks);
        await Task.Delay(300);

        // Assert
        var allOffsets = _capturedBatches
            .SelectMany(b => b.Records)
            .Select(r => r.Offset)
            .ToHashSet();

        allOffsets.Should().HaveCount(taskCount, "each record should have unique offset");

        var expectedOffsets = Enumerable.Range(0, taskCount).Select(i => (ulong)i).ToHashSet();
        allOffsets.Should()
            .BeEquivalentTo(expectedOffsets, "offsets should be sequential from 0 to {0}", taskCount - 1);
    }

    [Fact]
    public async Task AppendAsync_Should_Handle_High_Throughput_Load()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(100));
        var taskCount = 1000;
        var tasks = new ConcurrentBag<Task>();

        // Act - High throughput scenario
        var startTime = DateTime.UtcNow;

        await Task.Run(() =>
        {
            Parallel.For(0, taskCount, i =>
            {
                var batchBytes = CreateBatchBytes(new byte[] { (byte)(i % 256) });
                var task = appender.AppendAsync(batchBytes).AsTask();
                tasks.Add(task);
            });
        });

        await Task.WhenAll(tasks);
        await Task.Delay(300);

        var duration = DateTime.UtcNow - startTime;

        // Assert
        var totalRecords = _capturedBatches.Sum(b => b.Records.Count);
        totalRecords.Should().Be(taskCount, "all records should be appended");
        duration.Should().BeLessThan(TimeSpan.FromSeconds(10), "should handle load efficiently");
    }

    [Fact]
    public async Task AppendAsync_Should_Handle_Concurrent_Appends_During_Segment_Roll()
    {
        // Arrange
        var rollCount = 0;
        var appendsBeforeRoll = 20;

        _segmentWriter.ShouldRoll().Returns(call =>
        {
            var shouldRoll = _capturedBatches.Sum(b => b.Records.Count) >= appendsBeforeRoll;
            return shouldRoll;
        });

        _segmentWriter.When(x => x.DisposeAsync())
            .Do(_ =>
            {
                rollCount++;
                Thread.Sleep(10);
            });

        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));
        var taskCount = 50;
        var tasks = new List<Task>();

        // Act - Append while rolling is happening
        for (int i = 0; i < taskCount; i++)
        {
            var batchBytes = CreateBatchBytes(new byte[] { (byte)i });
            tasks.Add(Task.Run(async () => await appender.AppendAsync(batchBytes)));
        }

        await Task.WhenAll(tasks);
        await Task.Delay(500);

        // Assert
        var totalRecords = _capturedBatches.Sum(b => b.Records.Count);
        totalRecords.Should().Be(taskCount, "all records should be written despite rolling");
        rollCount.Should().BeGreaterThan(0, "segment should have rolled");
    }

    [Fact]
    public async Task AppendAsync_Should_Not_Lose_Data_Under_Race_Conditions()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));
        var producerCount = 10;
        var messagesPerProducer = 20;
        var allPayloads = new ConcurrentBag<byte>();

        // Act - Multiple producers sending messages simultaneously
        var producerTasks = Enumerable.Range(0, producerCount).Select(producerId =>
        {
            return Task.Run(async () =>
            {
                for (int i = 0; i < messagesPerProducer; i++)
                {
                    var payload = (byte)((producerId * messagesPerProducer) + i);
                    allPayloads.Add(payload);
                    await appender.AppendAsync(CreateBatchBytes(new byte[] { payload }));

                    if (i % 5 == 0)
                    {
                        await Task.Delay(1);
                    }
                }
            });
        }).ToArray();

        await Task.WhenAll(producerTasks);
        await Task.Delay(300);

        // Assert
        var totalRecords = _capturedBatches.Sum(b => b.Records.Count);
        totalRecords.Should().Be(producerCount * messagesPerProducer, "no data should be lost");

        var writtenPayloads = _capturedBatches
            .SelectMany(b => b.Records)
            .Select(r => r.Payload.ToArray()[0])
            .OrderBy(p => p)
            .ToList();

        var expectedPayloads = allPayloads.OrderBy(p => p).ToList();
        writtenPayloads.Should().BeEquivalentTo(expectedPayloads, "all payloads should be preserved");
    }

    [Fact]
    public async Task AppendAsync_Should_Handle_Channel_Pressure_From_Multiple_Threads()
    {
        // Arrange - Slow flush, fast appends
        var slowFlushDelay = TimeSpan.FromMilliseconds(500);
        var appender = CreateAppender(flushInterval: slowFlushDelay);
        var taskCount = 20;

        // Act
        var appendTasks = Enumerable.Range(0, taskCount)
            .Select(i => Task.Run(async () => await appender.AppendAsync(CreateBatchBytes(new byte[] { (byte)i }))))
            .ToList();

        var completionTask = Task.WhenAll(appendTasks);
        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));

        var completed = await Task.WhenAny(completionTask, timeoutTask);

        // Assert
        completed.Should().Be(completionTask, "should not deadlock under channel pressure");
        await Task.Delay(slowFlushDelay + TimeSpan.FromMilliseconds(100));

        var totalRecords = _capturedBatches.Sum(b => b.Records.Count);
        totalRecords.Should().Be(taskCount);
    }

    [Fact]
    public async Task AppendAsync_Should_Maintain_Order_Within_Single_Thread()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(200));
        var count = 30;

        // Act - Sequential appends from one thread
        for (int i = 0; i < count; i++)
        {
            await appender.AppendAsync(CreateBatchBytes(new byte[] { (byte)i }));
        }

        await Task.Delay(300);

        // Assert
        var records = _capturedBatches
            .SelectMany(b => b.Records)
            .OrderBy(r => r.Offset)
            .ToList();

        for (int i = 0; i < count; i++)
        {
            records[i].Payload.ToArray()[0].Should().Be((byte)i,
                "records should maintain order within single thread");
        }
    }

    [Fact]
    public async Task AppendAsync_Should_Handle_Mixed_Fast_And_Slow_Producers()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(100));
        var fastProducerCount = 10;
        var slowProducerCount = 5;

        // Act
        var fastProducers = Enumerable.Range(0, fastProducerCount).Select(i =>
            Task.Run(async () =>
            {
                for (int j = 0; j < 10; j++)
                {
                    await appender.AppendAsync(CreateBatchBytes(new byte[] { (byte)i }));
                }
            })
        );

        var slowProducers = Enumerable.Range(0, slowProducerCount).Select(i =>
            Task.Run(async () =>
            {
                for (int j = 0; j < 10; j++)
                {
                    await appender.AppendAsync(CreateBatchBytes(new byte[] { (byte)(i + 100) }));
                    await Task.Delay(10);
                }
            })
        );

        await Task.WhenAll(fastProducers.Concat(slowProducers));
        await Task.Delay(500);

        // Assert
        var totalRecords = _capturedBatches.Sum(b => b.Records.Count);
        totalRecords.Should().Be((fastProducerCount + slowProducerCount) * 10);
    }

    [Fact]
    public async Task AppendAsync_Should_Handle_Concurrent_Appends_And_Dispose()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(100));
        var appendCount = 30;
        var appendTasks = new List<Task>();

        // Act - Start appending
        for (int i = 0; i < appendCount; i++)
        {
            var batchBytes = CreateBatchBytes(new byte[] { (byte)i });
            appendTasks.Add(Task.Run(async () =>
            {
                try
                {
                    await appender.AppendAsync(batchBytes);
                }
                catch (ObjectDisposedException)
                {

                }
            }));
        }

        await Task.Delay(50);
        await appender.DisposeAsync();

        await Task.WhenAll(appendTasks);

        // Assert - Some records should have been written before dispose
        var totalRecords = _capturedBatches.Sum(b => b.Records.Count);
        totalRecords.Should().BeGreaterThan(0, "some records should be written before dispose");
    }

    private byte[] CreateBatchBytes(params byte[][] payloads)
    {

        var records = new List<LogRecord>();
        var currentTime = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        
        for (int i = 0; i < payloads.Length; i++)
        {
            records.Add(new LogRecord(
                0,
                currentTime + (ulong)i,
                payloads[i]
            ));
        }

        var batch = new LogRecordBatch(
            CommitLogMagicNumbers.LogRecordBatchMagicNumber,
            0,
            records,
            false
        );

        using var ms = new MemoryStream();
        var recordWriter = new LogRecordBinaryReader();
        var compressor = new NoopCompressor();
        var encoding = Encoding.UTF8;
        var batchWriter = new LogRecordBatchBinaryWriter(new LogRecordBinaryWriter(), compressor, encoding);
        batchWriter.WriteTo(batch, ms);
        return ms.ToArray();
    }

    private BinaryCommitLogAppender CreateAppender(
        ulong baseOffset = 0,
        TimeSpan? flushInterval = null)
    {
        return new BinaryCommitLogAppender(
            _segmentFactory,
            _testDirectory,
            baseOffset,
            flushInterval ?? TimeSpan.FromMilliseconds(100),
            _segmentRegistry
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

            }
        }
    }
}
