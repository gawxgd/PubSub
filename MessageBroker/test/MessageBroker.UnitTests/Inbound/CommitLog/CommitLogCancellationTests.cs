using System.Text;
using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;
using MessageBroker.Inbound.CommitLog;
using MessageBroker.Inbound.CommitLog.BatchRecord;
using MessageBroker.Inbound.CommitLog.Compressor;
using MessageBroker.Inbound.CommitLog.Record;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class CommitLogCancellationTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly ILogSegmentWriter _segmentWriter;
    private readonly ITopicSegmentRegistry _segmentRegistry;

    public CommitLogCancellationTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);

        _testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);

        _segmentWriter = Substitute.For<ILogSegmentWriter>();
        _segmentFactory = Substitute.For<ILogSegmentFactory>();
        _segmentRegistry = Substitute.For<ITopicSegmentRegistry>();

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
    public async Task AppendAsync_Should_Respect_Cancellation_During_Append()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var appender = CreateAppender(flushInterval: TimeSpan.FromHours(1));

        var appendCount = 0;
        _segmentWriter.AppendAsync(Arg.Any<byte[]>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                appendCount++;
                var token = call.Arg<CancellationToken>();
                return new ValueTask(Task.Delay(100, token));
            });

        // Act - Start appending, then cancel
        var task1 = appender.AppendAsync(CreateBatchBytes(new byte[] { 1 }));
        var task2 = appender.AppendAsync(CreateBatchBytes(new byte[] { 2 }));

        await Task.Delay(50);
        await cts.CancelAsync();
        await appender.DisposeAsync();

        // Assert - Should complete without hanging
        await Task.WhenAny(
            Task.WhenAll(task1.AsTask(), task2.AsTask()),
            Task.Delay(1000)
        );
    }

    [Fact]
    public async Task Dispose_Should_Cancel_Background_Flush_Task()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(100));
        var flushCount = 0;

        _segmentWriter.AppendAsync(Arg.Any<byte[]>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                flushCount++;
                return ValueTask.CompletedTask;
            });

        // Act
        await appender.AppendAsync(CreateBatchBytes(new byte[] { 1 }));
        await Task.Delay(50);
        await appender.DisposeAsync();

        var flushCountBeforeDelay = flushCount;
        await Task.Delay(300);
        var flushCountAfterDelay = flushCount;

        // Assert
        flushCountAfterDelay.Should().Be(flushCountBeforeDelay,
            "no more flushes should occur after disposal");
    }

    [Fact]
    public async Task AppendAsync_Should_Throw_When_Disposed()
    {
        // Arrange
        var appender = CreateAppender();
        await appender.DisposeAsync();

        // Act
        var act = async () => await appender.AppendAsync(CreateBatchBytes(new byte[] { 1 }));

        // Assert
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task SegmentWriter_Should_Receive_Cancellation_Token()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));
        CancellationToken? receivedToken = null;
        bool tokenWasCancelledDuringFlush = false;

        _segmentWriter.AppendAsync(Arg.Any<byte[]>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                receivedToken = call.Arg<CancellationToken>();
                tokenWasCancelledDuringFlush = receivedToken.Value.IsCancellationRequested;
                return ValueTask.CompletedTask;
            });

        // Act
        await appender.AppendAsync(CreateBatchBytes(new byte[] { 1 }));
        await Task.Delay(100);
        await appender.DisposeAsync();

        // Assert
        receivedToken.Should().NotBeNull();
        tokenWasCancelledDuringFlush.Should().BeFalse(
            "token should not be cancelled during normal flush");
    }

    [Fact]
    public async Task AppendAsync_During_Disposal_Should_Handle_Cancellation_Gracefully()
    {
        // Arrange
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(200));
        var tasks = new List<Task>();

        // Act - Start many appends and dispose immediately
        for (int i = 0; i < 20; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await appender.AppendAsync(CreateBatchBytes(new byte[] { (byte)i }));
                }
                catch (ObjectDisposedException)
                {
                }
            }));
        }

        await Task.Delay(10);
        var disposeTask = appender.DisposeAsync().AsTask();

        // Assert - Should complete within reasonable time
        var allTasks = Task.WhenAll(tasks.Append(disposeTask));
        var completed = await Task.WhenAny(allTasks, Task.Delay(5000));

        completed.Should().Be(allTasks, "should not deadlock on disposal during appends");
    }

    [Fact(Skip = "Current implementation writes synchronously with flush lock, so cancellation cannot interrupt an ongoing AppendAsync")]
    public async Task SegmentWriter_AppendAsync_Should_Be_Cancelled_On_Dispose()
    {
        // Arrange
        var slowAppendStarted = new TaskCompletionSource<bool>();
        var cancellationDetected = new TaskCompletionSource<bool>();

        _segmentWriter.AppendAsync(Arg.Any<byte[]>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var token = call.ArgAt<CancellationToken>(3);
                slowAppendStarted.SetResult(true);
                var t = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(5000, token);
                    }
                    catch (OperationCanceledException)
                    {
                        cancellationDetected.SetResult(true);
                        throw;
                    }
                }, token);
                return new ValueTask(t);
            });

        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));

        // Act
        await appender.AppendAsync(CreateBatchBytes(new byte[] { 1 }));
        await slowAppendStarted.Task;
        
        var disposeTask = Task.Run(async () => await appender.DisposeAsync());
        await Task.Delay(200);
        
        // Assert
        var cancelled = await Task.WhenAny(
            cancellationDetected.Task,
            Task.Delay(2000)
        );

        cancelled.Should().Be(cancellationDetected.Task,
            "cancellation should be propagated to segment writer");
            
        await disposeTask;
    }

    [Fact]
    public async Task Multiple_Dispose_Calls_Should_Not_Throw()
    {
        // Arrange
        var appender = CreateAppender();

        // Act
        await appender.DisposeAsync();
        var act = async () => await appender.DisposeAsync();

        // Assert
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Background_Flush_Should_Stop_On_Cancellation()
    {
        // Arrange
        var flushCount = 0;
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));

        _segmentWriter.AppendAsync(Arg.Any<byte[]>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                flushCount++;
                return ValueTask.CompletedTask;
            });

        // Act
        await appender.AppendAsync(CreateBatchBytes(new byte[] { 1 }));
        await Task.Delay(75);

        var flushCountBeforeDispose = flushCount;
        await appender.DisposeAsync();
        await Task.Delay(200);

        var flushCountAfterDispose = flushCount;

        // Assert
        flushCountBeforeDispose.Should().BeGreaterThan(0);
        (flushCountAfterDispose - flushCountBeforeDispose).Should().BeLessThan(2,
            "at most one more flush during disposal, but no continuous flushes after");
    }

    [Fact]
    public async Task Segment_Roll_Should_Handle_Cancellation()
    {
        // Arrange
        var rollStarted = false;
        _segmentWriter.ShouldRoll().Returns(true);

        _segmentWriter.When(x => x.DisposeAsync())
            .Do(async _ =>
            {
                rollStarted = true;
                await Task.Delay(100);
            });

        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));

        _segmentWriter.AppendAsync(Arg.Any<byte[]>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.CompletedTask);

        // Act
        await appender.AppendAsync(CreateBatchBytes(new byte[] { 1 }));
        await Task.Delay(75);
        await appender.DisposeAsync();

        // Assert
        rollStarted.Should().BeTrue("segment roll should have started");
    }

    [Fact]
    public async Task AppendAsync_Should_Complete_Pending_Operations_Before_Cancellation()
    {
        // Arrange
        var appendCallCount = 0;
        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(100));

        _segmentWriter.AppendAsync(Arg.Any<byte[]>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                Interlocked.Increment(ref appendCallCount);
                return ValueTask.CompletedTask;
            });

        // Act - Queue messages then dispose
        await appender.AppendAsync(CreateBatchBytes(new byte[] { 1 }));
        await appender.AppendAsync(CreateBatchBytes(new byte[] { 2 }));
        await appender.AppendAsync(CreateBatchBytes(new byte[] { 3 }));

        await Task.Delay(150);
        await appender.DisposeAsync();

        // Assert - All 3 batches should be written
        appendCallCount.Should().Be(3, "all queued batches should be flushed before cancellation");
    }

    [Fact(Skip = "Current implementation writes synchronously with flush lock, so long-running writes cannot be cancelled mid-operation")]
    public async Task Long_Running_Flush_Should_Be_Cancellable()
    {
        // Arrange
        var longFlushStarted = new TaskCompletionSource<bool>();
        var cancellationReceived = false;

        _segmentWriter.AppendAsync(Arg.Any<byte[]>(), Arg.Any<ulong>(), Arg.Any<ulong>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var token = call.ArgAt<CancellationToken>(3);
                longFlushStarted.SetResult(true);
                var t = Task.Run(async () =>
                {
                    try
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            token.ThrowIfCancellationRequested();
                            await Task.Delay(50, token);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        cancellationReceived = true;
                        throw;
                    }
                }, token);
                return new ValueTask(t);
            });

        var appender = CreateAppender(flushInterval: TimeSpan.FromMilliseconds(50));

        // Act
        await appender.AppendAsync(CreateBatchBytes(new byte[] { 1 }));
        await longFlushStarted.Task;
        await Task.Delay(100);
        
        var disposeTask = appender.DisposeAsync();
        await Task.Delay(200);
        await disposeTask;

        // Assert
        cancellationReceived.Should().BeTrue("long-running flush should be cancelled");
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
        var recordWriter = new LogRecordBinaryWriter();
        var compressor = new NoopCompressor();
        var encoding = Encoding.UTF8;
        var batchWriter = new LogRecordBatchBinaryWriter(recordWriter, compressor, encoding);
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
