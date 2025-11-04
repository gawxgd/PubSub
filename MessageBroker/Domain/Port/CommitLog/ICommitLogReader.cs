using MessageBroker.Domain.Entities.CommitLog;

namespace MessageBroker.Domain.Port.CommitLog;

public interface ICommitLogReader : IAsyncDisposable
{
    /// <summary>
    /// Read a single record at the specified offset.
    /// </summary>
    LogRecord? ReadRecord(ulong offset);

    /// <summary>
    /// Read records starting from offset, returning up to maxRecords.
    /// </summary>
    LogRecordBatch? ReadRecordBatch(ulong baseOffset);

    /// <summary>
    /// Read records from a timestamp onwards.
    /// </summary>
    IEnumerable<LogRecord> ReadFromTimestamp(ulong timestamp);

    /// <summary>
    /// Get the current highest offset in the log.
    /// </summary>
    ulong GetHighWaterMark();
}