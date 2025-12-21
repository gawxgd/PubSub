using System.Collections.Concurrent;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;

namespace MessageBroker.Inbound.CommitLog;

public sealed class BinaryCommitLogReader : ICommitLogReader
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<BinaryCommitLogReader>(LogSource.MessageBroker);

    private readonly ILogSegmentFactory _segmentFactory;
    private readonly ITopicSegmentRegistry _segmentRegistry;
    private ILogSegmentReader? _activeSegmentReader;
    private ulong _currentSegmentBaseOffset;
    private readonly ConcurrentDictionary<ulong, ILogSegmentReader> _inactiveSegmentReaders = new();

    public BinaryCommitLogReader(ILogSegmentFactory segmentFactory, ITopicSegmentRegistry segmentRegistry)
    {
        _segmentFactory = segmentFactory;
        _segmentRegistry = segmentRegistry;
        RefreshSegmentReader();
    }

    private void RefreshSegmentReader()
    {
        var activeSegment = _segmentRegistry.GetActiveSegment();

        if (_activeSegmentReader == null || _currentSegmentBaseOffset != activeSegment.BaseOffset)
        {
            _activeSegmentReader?.DisposeAsync().AsTask().Wait();
            _activeSegmentReader = _segmentFactory.CreateReader(activeSegment);
            _currentSegmentBaseOffset = activeSegment.BaseOffset;
        }
    }

    public LogRecord? ReadRecord(ulong offset)
    {
        var batch = ReadRecordBatch(offset);

        if (batch == null)
        {
            Logger.LogWarning($"No log segments found for {offset}");
            return null;
        }

        var record = batch.Records.FirstOrDefault(r => r.Offset == offset);

        if (record == null)
        {
            Logger.LogWarning($"Batch does not contain record with {offset}");
            return null;
        }

        return record;
    }

    public LogRecordBatch? ReadRecordBatch(ulong baseOffset)
    {
        var segment = _segmentRegistry.GetSegmentContainingOffset(baseOffset);

        if (segment == null)
        {
            Logger.LogWarning($"Cannot find segment containing offset {baseOffset}");
            return null;
        }

        var segmentReader = GetOrCreateSegmentReader(segment);
        var batch = segmentReader.ReadBatch(baseOffset);

        if (batch == null)
        {
            Logger.LogWarning($"Read batch for {baseOffset} returned null");
            return null;
        }

        return batch;
    }

    public byte[]? ReadBatchBytes(ulong baseOffset)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<LogRecord> ReadFromTimestamp(ulong timestamp)
    {
        // RefreshSegmentReader();
        // using var enumerator = _activeSegmentReader!.ReadFromTimestamp(timestamp).GetEnumerator();
        // if (!enumerator.MoveNext()) yield break;
        // var batch = enumerator.Current;
        // foreach (var record in batch.Records)
        // {
        //     if (record.Timestamp >= timestamp)
        //     {
        //         yield return record;
        //     }
        // }
        throw new NotImplementedException();
    }

    public ulong GetHighWaterMark()
    {
        return _segmentRegistry.GetHighWaterMark();
    }

    public async ValueTask DisposeAsync()
    {
        if (_activeSegmentReader != null)
            await _activeSegmentReader.DisposeAsync();
    }

    private ILogSegmentReader GetOrCreateSegmentReader(LogSegment segment)
    {
        return _inactiveSegmentReaders.GetOrAdd(segment.BaseOffset, (_) => _segmentFactory.CreateReader(segment));
    }
}