using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
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
    private readonly AssignOffsetsUseCase _assignOffsetsUseCase = new();

    private sealed record AppendRequest(ReadOnlyMemory<byte> Payload, TaskCompletionSource<ulong> Completion);

    // Append requests are enqueued; a periodic flush drains the queue and performs disk IO.
    // ACK is returned only after the queued item is actually written + flushed.
    private readonly Channel<AppendRequest> _batchChannel =
        Channel.CreateBounded<AppendRequest>(new BoundedChannelOptions(100) //ToDo get from options
        {
            SingleWriter = false,
            // Both AppendAsync and the periodic flush may drain the channel, but they are serialized by _flushLock.
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait
        });

    private readonly Task _backgroundFlushTask;
    private readonly CancellationTokenSource _cancellationTokenSource;

    private static readonly IAutoLogger
        Logger = AutoLoggerFactory.CreateLogger<BinaryCommitLogAppender>(LogSource.MessageBroker);

    public BinaryCommitLogAppender(ILogSegmentFactory segmentFactory, string directory, ulong currentOffset,
        TimeSpan flushInterval, ITopicSegmentRegistry segmentRegistry)
    {
        _directory = directory;
        _segmentFactory = segmentFactory;
        _activeSegment = segmentRegistry.GetActiveSegment();
        _activeSegmentWriter = segmentFactory.CreateWriter(_activeSegment);
        _currentOffset = currentOffset;
        _flushInterval = flushInterval;
        _cancellationTokenSource = new CancellationTokenSource();
        _segmentRegistry = segmentRegistry;
        _segmentRegistry.UpdateCurrentOffset(_currentOffset);
        _backgroundFlushTask = StartBackgroundFlushAsync();
    }

    public async ValueTask<ulong> AppendAsync(ReadOnlyMemory<byte> payload)
    {
        // Enqueue first (FIFO), then flush the channel (older messages first), then await completion.
        // ACK is returned only after THIS request is physically written+flushed by a channel drain.
        var tcs = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        var req = new AppendRequest(payload, tcs);

        // IMPORTANT: enqueue outside _flushLock to avoid deadlock when the bounded channel is full.
        // If the channel is full, we must allow the background flusher (or another thread) to drain it.
        await _batchChannel.Writer.WriteAsync(req, _cancellationTokenSource.Token)
            .ConfigureAwait(false);

        // Preserve the "existing logic": under the lock, drain all queued messages now.
        // This ensures our request is flushed after everything that was queued before it,
        // without waiting for the periodic flush interval.
        await FlushChannelToLogSegmentAsync(CancellationToken.None).ConfigureAwait(false);

        return await req.Completion.Task.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            // Stop accepting new work and stop the periodic flush loop.
            _batchChannel.Writer.TryComplete();
            await _cancellationTokenSource.CancelAsync();
            await _backgroundFlushTask;

            // Best-effort final drain without cancellation.
            await FlushChannelToLogSegmentAsync(CancellationToken.None).ConfigureAwait(false);

            if (_activeSegmentWriter is IAsyncDisposable writer)
            {
                await writer.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            Logger.LogError("Error while disposing BinaryCommitLogAppender", ex);
        }
        finally
        {
            // Release any pending callers if we are disposing while work is still queued.
            while (_batchChannel.Reader.TryRead(out var pending))
            {
                pending.Completion.TrySetException(new ObjectDisposedException(nameof(BinaryCommitLogAppender)));
            }
            _flushLock.Dispose();
            _cancellationTokenSource.Dispose();
        }
    }

    private async Task FlushChannelToLogSegmentAsync(CancellationToken token)
    {
        await _flushLock.WaitAsync();
        try
        {
            while (_batchChannel.Reader.TryRead(out var req))
            {
                try
                {
                    var baseOffset = await WriteToSegment(req.Payload, token).ConfigureAwait(false);
                    req.Completion.TrySetResult(baseOffset);
                }
                catch (Exception ex)
                {
                    req.Completion.TrySetException(ex);
                }
            }

            _segmentRegistry.UpdateCurrentOffset(_currentOffset);
        }
        finally
        {
            _flushLock.Release();
        }
    }

    private async Task<ulong> WriteToSegment(ReadOnlyMemory<byte> message, CancellationToken token)
    {
        var batchBaseOffset = _currentOffset;
        var batch = message.ToArray();

        var nextOffset = _assignOffsetsUseCase.AssignOffsets(batchBaseOffset, batch);
        _currentOffset = nextOffset;
        var batchLastOffset = nextOffset - 1;

        if (ShouldRollActiveSegment())
        {
            await RollActiveSegmentAsync(batchBaseOffset).ConfigureAwait(false);
        }

        await _activeSegmentWriter.AppendAsync(batch, batchBaseOffset, batchLastOffset, token)
            .ConfigureAwait(false);

        return batchBaseOffset;
    }

    private bool ShouldRollActiveSegment()
    {
        return _activeSegmentWriter.ShouldRoll();
    }

    private async Task RollActiveSegmentAsync(ulong newSegmentBaseOffset)
    {
        await _activeSegmentWriter.DisposeAsync().ConfigureAwait(false);
        var newSegment = _segmentFactory.CreateLogSegment(_directory, newSegmentBaseOffset);
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
                await FlushChannelToLogSegmentAsync(_cancellationTokenSource.Token).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is TaskCanceledException or OperationCanceledException)
        {
            Logger.LogDebug("Flush task cancelled");
        }
    }
}