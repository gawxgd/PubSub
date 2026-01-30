using MessageBroker.Domain.Entities.CommitLog;
using System.Threading.Channels;

namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogReader : IAsyncDisposable
{

    LogRecord? ReadRecord(ulong offset);

    LogRecordBatch? ReadRecordBatch(ulong baseOffset);

    (byte[] batchBytes, ulong baseOffset, ulong lastOffset)? ReadBatchBytes(ulong baseOffset);

    IEnumerable<LogRecord> ReadFromTimestamp(ulong timestamp);

    ulong GetHighWaterMark();
}
