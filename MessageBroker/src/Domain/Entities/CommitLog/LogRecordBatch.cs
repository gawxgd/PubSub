namespace MessageBroker.Domain.Entities.CommitLog;

public sealed record LogRecordBatch(
    CommitLogMagicNumbers MagicNumber,
    ulong BaseOffset, //ToDo delete from writing
    ICollection<LogRecord> Records,
    bool Compressed)
{
    public ulong BaseTimestamp => Records.Min(r => r.Timestamp);

    public ulong LastOffset => Records.Last().Offset;
}