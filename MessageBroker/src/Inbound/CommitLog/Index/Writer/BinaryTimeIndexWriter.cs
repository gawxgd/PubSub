using System.Buffers.Binary;
using MessageBroker.Domain.Entities.CommitLog.Index;
using MessageBroker.Domain.Port.CommitLog.Index.Writer;

namespace MessageBroker.Inbound.CommitLog.Index.Writer;

public sealed class BinaryTimeIndexWriter : ITimeIndexWriter
{
    public async Task WriteToAsync(TimeIndexEntry entry, Stream stream)
    {
        Span<byte> buffer = stackalloc byte[TimeIndexEntry.Size];
        BinaryPrimitives.WriteUInt64BigEndian(buffer[..8], entry.Timestamp);
        BinaryPrimitives.WriteUInt64BigEndian(buffer.Slice(8, 8), entry.FilePosition);
        await stream.WriteAsync(buffer.ToArray().AsMemory()).ConfigureAwait(false);
        await stream.FlushAsync().ConfigureAwait(false);
    }
}