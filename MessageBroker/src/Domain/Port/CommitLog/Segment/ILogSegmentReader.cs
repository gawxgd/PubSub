using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog.Segment;

public interface ILogSegmentReader : IAsyncDisposable
{
    (byte[] batchBytes, ulong baseOffset, ulong lastOffset)? ReadBatchBytes(ulong offset);
    LogRecordBatch? ReadBatch(ulong offset);
    IEnumerable<LogRecordBatch> ReadRange(ulong startOffset, ulong endOffset);
    IEnumerable<LogRecordBatch> ReadFromTimestamp(ulong timestamp);
    
    ulong RecoverHighWaterMark();
}
