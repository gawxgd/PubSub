using System.Threading.Channels;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using MessageBroker.Domain.Port.CommitLog.Segment;

namespace MessageBroker.Inbound.CommitLog;

public sealed class BinaryCommitLogAppender : ICommitLogAppender
{
    //ToDo background flush
    // and rolling segments
    private readonly LogSegment _activeSegment;
    private readonly ILogSegmentWriter _activeSegmentWriter;
    private ulong _currentOffset;
    private readonly SemaphoreSlim _flushLock = new(1, 1);
    private readonly TimeSpan _flushInterval;
    private readonly Channel<ReadOnlyMemory<byte>> _batchChannel =
        Channel.CreateBounded<ReadOnlyMemory<byte>>(new BoundedChannelOptions(10) //ToDo get from options
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });
    private Task _backgroundFlushTask;
    private CancellationTokenSource _cancellationTokenSource;
    
    public BinaryCommitLogAppender(ILogSegmentFactory segmentFactory, string directory, ulong baseOffset, TimeSpan flushInterval)
    {
        _activeSegment = segmentFactory.CreateLogSegment(directory, baseOffset);
        _activeSegmentWriter = segmentFactory.CreateWriter(_activeSegment);
        _currentOffset = baseOffset;
        _flushInterval = flushInterval;
        _cancellationTokenSource = new CancellationTokenSource();
        _backgroundFlushTask = StartBackgroundFlushAsync();
    }

    public async ValueTask AppendAsync(ReadOnlyMemory<byte> payload)
    {
        if (!_batchChannel.Writer.TryWrite(payload))
        {
           await FlushChannelToLogSegmentAsync();
           
           await _batchChannel.Writer.WriteAsync(payload, _cancellationTokenSource.Token);
        }
    }

    private async Task FlushChannelToLogSegmentAsync()
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

        await _activeSegmentWriter.AppendAsync(recordBatch, _cancellationTokenSource.Token);
    }
    
    private async Task StartBackgroundFlushAsync()
    {
        try
        {
            while (_cancellationTokenSource.Token.IsCancellationRequested)
            {
                await Task.Delay(_flushInterval, _cancellationTokenSource.Token);
                await FlushChannelToLogSegmentAsync();
            }
        }
        catch (TaskCanceledException)
        {
        }
    }
}