using MessageBroker.Domain.Entities.CommitLog;
using MessageBroker.Domain.Port.CommitLog;
using MessageBroker.Domain.Port.CommitLog.Segment;

namespace MessageBroker.Inbound.CommitLog;

public sealed class BinaryCommitLogReader : ICommitLogReader
{
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly TopicSegmentManager _segmentManager;
    private readonly string _topic;
    private ILogSegmentReader? _segmentReader;
    private ulong _currentSegmentBaseOffset;

    public BinaryCommitLogReader(ILogSegmentFactory segmentFactory, TopicSegmentManager segmentManager, string topic)
    {
        _segmentFactory = segmentFactory;
        _segmentManager = segmentManager;
        _topic = topic;
        RefreshSegmentReader();
    }

    private void RefreshSegmentReader()
    {
        var activeSegment = _segmentManager.GetActiveSegment();

        if (_segmentReader == null || _currentSegmentBaseOffset != activeSegment.BaseOffset)
        {
            _segmentReader?.DisposeAsync().AsTask().Wait();
            _segmentReader = _segmentFactory.CreateReader(activeSegment);
            _currentSegmentBaseOffset = activeSegment.BaseOffset;
        }
    }

    public LogRecord? ReadRecord(ulong offset)
    {
        RefreshSegmentReader();
        var batch = _segmentReader!.ReadBatch(offset);
        return batch?.Records.FirstOrDefault(r => r.Offset == offset);
    }

    public IEnumerable<LogRecord> ReadRecords(ulong startOffset, int maxRecords)
    {
        RefreshSegmentReader();
        var recordCount = 0;
        var highWaterMark = _segmentManager.GetHighWaterMark();

        foreach (var batch in _segmentReader!.ReadRange(startOffset, highWaterMark))
        {
            foreach (var record in batch.Records)
            {
                if (record.Offset >= startOffset)
                {
                    yield return record;
                    if (++recordCount >= maxRecords) yield break;
                }
            }
        }
    }

    public IEnumerable<LogRecord> ReadFromTimestamp(ulong timestamp, int maxRecords)
    {
        RefreshSegmentReader();
        var recordCount = 0;

        foreach (var batch in _segmentReader!.ReadFromTimestamp(timestamp))
        {
            foreach (var record in batch.Records)
            {
                if (record.Timestamp >= timestamp)
                {
                    yield return record;
                    if (++recordCount >= maxRecords) yield break;
                }
            }
        }
    }

    public ulong GetHighWaterMark()
    {
        return _segmentManager.GetHighWaterMark();
    }

    public async ValueTask DisposeAsync()
    {
        if (_segmentReader != null)
            await _segmentReader.DisposeAsync();
    }
}