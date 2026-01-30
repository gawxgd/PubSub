using System.Buffers.Binary;

namespace MessageBroker.Domain.Entities.CommitLog.Index;

public sealed record OffsetIndexEntry(ulong RelativeOffset, ulong FilePosition)
{
    public static int Size => sizeof(ulong) + sizeof(ulong);
    
}
