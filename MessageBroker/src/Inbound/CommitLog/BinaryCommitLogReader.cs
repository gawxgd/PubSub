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
        return ReadFromSegment(
            baseOffset,
            (reader, offset) => reader.ReadBatch(offset),
            "Read batch");
    }

    public (byte[] batchBytes, ulong baseOffset, ulong lastOffset)? ReadBatchBytes(ulong offset)
    {
        return ReadFromSegment(
            offset,
            (reader, off) => reader.ReadBatchBytes(off),
            "Read batch bytes");
    }

    public IEnumerable<LogRecord> ReadFromTimestamp(ulong timestamp)
    {

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

    private T? ReadFromSegment<T>(
        ulong offset,
        Func<ILogSegmentReader, ulong, T?> readFunc,
        string operationName)
    {
        var segment = _segmentRegistry.GetSegmentContainingOffset(offset);

        if (segment == null)
        {
            Logger.LogWarning($"Cannot find segment containing offset {offset}");
            return default;
        }

        var segmentReader = GetOrCreateSegmentReader(segment);
        var result = readFunc(segmentReader, offset);

        if (result == null)
        {
            Logger.LogWarning($"{operationName} for {offset} returned null");
            return default;
        }

        return result;
    }

    private ILogSegmentReader GetOrCreateSegmentReader(LogSegment segment)
    {
        return _inactiveSegmentReaders.GetOrAdd(segment.BaseOffset, (_) => _segmentFactory.CreateReader(segment));
    }
}
