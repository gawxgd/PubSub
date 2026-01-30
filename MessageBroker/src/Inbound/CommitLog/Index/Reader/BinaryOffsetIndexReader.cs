using System.Buffers.Binary;
using MessageBroker.Domain.Entities.CommitLog.Index;
using MessageBroker.Domain.Port.CommitLog.Index.Reader;

namespace MessageBroker.Inbound.CommitLog.Index.Reader;

public sealed class BinaryOffsetIndexReader : IOffsetIndexReader
{
    public OffsetIndexEntry ReadFrom(Stream stream)
    {
        var entrySize = OffsetIndexEntry.Size;
        Span<byte> buffer = stackalloc byte[entrySize];
        var bytesRead = stream.Read(buffer);

        if (bytesRead != entrySize)
        {
            throw new InvalidDataException(
                $"Failed to read complete offset index entry. Expected {entrySize} bytes, got {bytesRead}");
        }

        var relativeOffset = BinaryPrimitives.ReadUInt64BigEndian(buffer[..8]);
        var filePosition = BinaryPrimitives.ReadUInt64BigEndian(buffer.Slice(8, 8));

        return new OffsetIndexEntry(relativeOffset, filePosition);
    }
}
