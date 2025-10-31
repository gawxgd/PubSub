using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Segment;

public interface ILogSegmentReader : IAsyncDisposable
{
    LogRecordBatch? ReadBatch(ulong offset);
    IEnumerable<LogRecordBatch> ReadRange(ulong startOffset, ulong endOffset);
    IEnumerable<LogRecordBatch> ReadFromTimestamp(ulong timestamp);
}