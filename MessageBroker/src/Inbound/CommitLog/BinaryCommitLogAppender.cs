using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;

namespace MessageBroker.Inbound.CommitLog;

public sealed class BinaryCommitLogAppender : ICommitLogAppender
{
    private LogSegment _activeSegment;
    private ILogSegmentWriter _activeSegmentWriter;
    private ulong _currentOffset;
    private readonly SemaphoreSlim _flushLock = new(1, 1);
    private readonly TimeSpan _flushInterval;
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly string _directory;
    private readonly ITopicSegmentRegistry _segmentRegistry;

    private readonly Channel<ReadOnlyMemory<byte>> _batchChannel =
        Channel.CreateBounded<ReadOnlyMemory<byte>>(new BoundedChannelOptions(10) //ToDo get from options
        {
            SingleWriter = false,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

    private readonly Task _backgroundFlushTask;
    private readonly CancellationTokenSource _cancellationTokenSource;

    private static readonly IAutoLogger
        Logger = AutoLoggerFactory.CreateLogger<BinaryCommitLogAppender>(LogSource.MessageBroker);

    public BinaryCommitLogAppender(ILogSegmentFactory segmentFactory, string directory, ulong baseOffset,
        TimeSpan flushInterval, ITopicSegmentRegistry segmentRegistry)
    {
        _directory = directory;
        _segmentFactory = segmentFactory;
        _activeSegment = segmentFactory.CreateLogSegment(directory, baseOffset);
        _activeSegmentWriter = segmentFactory.CreateWriter(_activeSegment);
        _currentOffset = baseOffset;
        _flushInterval = flushInterval;
        _cancellationTokenSource = new CancellationTokenSource();
        _segmentRegistry = segmentRegistry;
        _segmentRegistry.UpdateActiveSegment(_activeSegment);
        _segmentRegistry.UpdateCurrentOffset(_currentOffset);
        _backgroundFlushTask = StartBackgroundFlushAsync();
    }

    public async ValueTask AppendAsync(ReadOnlyMemory<byte> payload)
    {
        // ToDo do a hybrid batching by channel count and batch size
        if (!_batchChannel.Writer.TryWrite(payload))
        {
            await FlushChannelToLogSegmentAsync();

            await _batchChannel.Writer.WriteAsync(payload, _cancellationTokenSource.Token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _cancellationTokenSource.CancelAsync();

            await _backgroundFlushTask;

            await FlushChannelToLogSegmentAsync();

            if (_activeSegmentWriter is IAsyncDisposable writer)
            {
                await writer.DisposeAsync();
            }
        }
        catch (Exception ex)
        {
            Logger.LogError("Error while disposing BinaryCommitLogAppender", ex);
        }
        finally
        {
            _batchChannel.Writer.TryComplete();
            _flushLock.Dispose();
            _cancellationTokenSource.Dispose();
        }
    }

    private async Task FlushChannelToLogSegmentAsync()
    {
        await _flushLock.WaitAsync();
        try
        {
            var batchBaseOffset = _currentOffset;
            var records = new List<LogRecord>();

            while (_batchChannel.Reader.TryRead(out var message))
            {
                records.Add(new LogRecord(
                    _currentOffset++,
                    (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), // TODO: extract timestamp from message
                    message
                ));
            }

            if (records.Count == 0) return;

            var recordBatch = new LogRecordBatch(
                CommitLogMagicNumbers.LogRecordBatchMagicNumber,
                batchBaseOffset,
                records,
                false
            );

            if (ShouldRollActiveSegment())
            {
                await RollActiveSegmentAsync();
            }

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token);

            await _activeSegmentWriter.AppendAsync(recordBatch, linkedCts.Token);

            _segmentRegistry.UpdateCurrentOffset(_currentOffset);
        }
        finally
        {
            _flushLock.Release();
        }
    }

    private bool ShouldRollActiveSegment()
    {
        return _activeSegmentWriter.ShouldRoll();
    }

    private async Task RollActiveSegmentAsync()
    {
        await _activeSegmentWriter.DisposeAsync();
        var newSegment = _segmentFactory.CreateLogSegment(_directory, _currentOffset);
        _activeSegmentWriter = _segmentFactory.CreateWriter(newSegment);
        _activeSegment = newSegment;
        _segmentRegistry.UpdateActiveSegment(newSegment);
    }

    private async Task StartBackgroundFlushAsync()
    {
        try
        {
            while (!_cancellationTokenSource.Token.IsCancellationRequested)
            {
                await Task.Delay(_flushInterval, _cancellationTokenSource.Token);
                await FlushChannelToLogSegmentAsync();
            }
        }
        catch (Exception ex) when (ex is TaskCanceledException or OperationCanceledException)
        {
            Logger.LogDebug("Flush task cancelled");
        }
    }
}