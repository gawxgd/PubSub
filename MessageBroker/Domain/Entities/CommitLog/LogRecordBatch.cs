namespace MessageBroker.Domain.Entities.CommitLog;

public sealed record LogRecordBatch(
    CommitLogMagicNumbers MagicNumber,
    ulong BaseOffset,
    ICollection<LogRecord> Records,
    bool Compressed)
{
    public ulong BaseTimestamp => Records.Min(r => r.Timestamp);
}