namespace MessageBroker.Domain.Entities.CommitLog.Index;

public sealed record TimeIndexEntry(ulong Timestamp, ulong FilePosition)
{
    public static int Size => sizeof(ulong) + sizeof(ulong);
}