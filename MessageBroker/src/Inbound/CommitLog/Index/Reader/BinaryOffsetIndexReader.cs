using System.Buffers.Binary;
using MessageBroker.Domain.Entities.CommitLog.Index;
using MessageBroker.Domain.Port.CommitLog.Index.Reader;

namespace MessageBroker.Inbound.CommitLog.Index.Reader;

public sealed class BinaryOffsetIndexReader : IOffsetIndexReader
{
    public OffsetIndexEntry ReadFrom(ReadOnlySpan<byte> data)
    {
        var relativeOffset = BinaryPrimitives.ReadUInt64BigEndian(data[..8]);
        var filePosition = BinaryPrimitives.ReadUInt64BigEndian(data.Slice(8, 8));

        return new OffsetIndexEntry(relativeOffset, filePosition);
    }
}
