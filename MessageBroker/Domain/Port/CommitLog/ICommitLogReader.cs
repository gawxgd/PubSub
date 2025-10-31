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
    IEnumerable<LogRecord> ReadRecords(ulong startOffset, int maxRecords);

    /// <summary>
    /// Read records from a timestamp onwards.
    /// </summary>
    IEnumerable<LogRecord> ReadFromTimestamp(ulong timestamp, int maxRecords);

    /// <summary>
    /// Get the current highest offset in the log.
    /// </summary>
    ulong GetHighWaterMark();
}