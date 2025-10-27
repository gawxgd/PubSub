using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Inbound.CommitLog;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class BinaryCommitLogAppenderTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly ILogSegmentWriter _segmentWriter;

    public BinaryCommitLogAppenderTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);
        
        _testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        
        _segmentWriter = Substitute.For<ILogSegmentWriter>();
        _segmentFactory = Substitute.For<ILogSegmentFactory>();
        
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
    }

    [Fact]
    public void Constructor_Should_Create_Initial_Segment()
    {
        // Act
        var appender = CreateAppender();

        // Assert
        _segmentFactory.Received(1).CreateLogSegment(_testDirectory, 0);
        _segmentFactory.Received(1).CreateWriter(Arg.Any<LogSegment>());
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

        // Assert
        await _segmentWriter.Received().AppendAsync(
            Arg.Is<LogRecordBatch>(b => b.Records.Count > 0),
            Arg.Any<CancellationToken>());
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

        // Assert
        await _segmentWriter.Received(1).AppendAsync(
            Arg.Is<LogRecordBatch>(b => b.Records.Count == 3),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task AppendAsync_Should_Roll_Segment_When_Needed()
    {
        // Arrange
        _segmentWriter.ShouldRoll().Returns(true);
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));

        // Act
        await appender.AppendAsync(new byte[] { 1 });
        await Task.Delay(150);

        // Assert
        await _segmentWriter.Received().DisposeAsync();
        _segmentFactory.Received(2).CreateWriter(Arg.Any<LogSegment>());
    }

    [Fact]
    public async Task AppendAsync_Should_Assign_Sequential_Offsets()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));
        LogRecordBatch? capturedBatch = null;
        
        _segmentWriter.AppendAsync(Arg.Any<LogRecordBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                capturedBatch = call.Arg<LogRecordBatch>();
                return ValueTask.CompletedTask;
            });

        // Act
        await appender.AppendAsync(new byte[] { 1 });
        await appender.AppendAsync(new byte[] { 2 });
        await appender.AppendAsync(new byte[] { 3 });
        await Task.Delay(150);

        // Assert
        capturedBatch.Should().NotBeNull();
        capturedBatch!.Records.Should().HaveCount(3);
        capturedBatch.Records.ElementAt(0).Offset.Should().Be(0);
        capturedBatch.Records.ElementAt(1).Offset.Should().Be(1);
        capturedBatch.Records.ElementAt(2).Offset.Should().Be(2);
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
        LogRecordBatch? capturedBatch = null;
        
        _segmentWriter.AppendAsync(Arg.Any<LogRecordBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                capturedBatch = call.Arg<LogRecordBatch>();
                return ValueTask.CompletedTask;
            });

        // Act
        await appender.AppendAsync(new byte[] { 1 });
        await Task.Delay(150);

        // Assert
        capturedBatch.Should().NotBeNull();
        capturedBatch!.Records.First().Timestamp.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task AppendAsync_Should_Preserve_Payload()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));
        var originalPayload = new byte[] { 1, 2, 3, 4, 5 };
        LogRecordBatch? capturedBatch = null;
        
        _segmentWriter.AppendAsync(Arg.Any<LogRecordBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                capturedBatch = call.Arg<LogRecordBatch>();
                return ValueTask.CompletedTask;
            });

        // Act
        await appender.AppendAsync(originalPayload);
        await Task.Delay(150);

        // Assert
        capturedBatch.Should().NotBeNull();
        capturedBatch!.Records.First().Payload.ToArray().Should().BeEquivalentTo(originalPayload);
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
            "test-topic"
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

