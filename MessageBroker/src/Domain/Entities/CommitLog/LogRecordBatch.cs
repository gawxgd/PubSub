namespace MessageBroker.Domain.Entities.CommitLog;

public sealed record LogRecordBatch(
    CommitLogMagicNumbers MagicNumber,
    ulong BaseOffset, //ToDo delete from writing
    ICollection<LogRecord> Records,
    bool Compressed)
{
    public ulong BaseTimestamp
    {
        get
        {
            if (Records == null || Records.Count == 0)
            {
                throw new InvalidOperationException("Cannot get BaseTimestamp from empty Records collection");
            }
            return Records.Min(r => r.Timestamp);
        }
    }

    public ulong LastOffset
    {
        get
        {
            if (Records == null || Records.Count == 0)
            {
                throw new InvalidOperationException("Cannot get LastOffset from empty Records collection");
            }
            return Records.Last().Offset;
        }
    }
}