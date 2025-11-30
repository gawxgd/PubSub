using MessageBroker.Domain.Entities.CommitLog;
using System.Threading.Channels;

namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogReader : IAsyncDisposable
{
    /// <summary>
    /// Read a single record at the specified offset.
    /// </summary>
    LogRecord? ReadRecord(ulong offset);

    /// <summary>
    /// Read record batch from baseOffset
    /// </summary>
    LogRecordBatch? ReadRecordBatch(ulong baseOffset);

    /// <summary>
    /// Not implemented
    /// </summary>
    IEnumerable<LogRecord> ReadFromTimestamp(ulong timestamp);

    /// <summary>
    /// Get the current highest offset in the log.
    /// </summary>
    ulong GetHighWaterMark();
    Task ReadAsync(Channel<byte[]> channel);
}