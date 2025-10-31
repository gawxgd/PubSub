namespace MessageBroker.Domain.Entities.CommitLog.Index;

public sealed record TimeIndexEntry(ulong Timestamp, ulong RelativeOffset)
{
    public static int Size => sizeof(ulong) + sizeof(ulong);
}