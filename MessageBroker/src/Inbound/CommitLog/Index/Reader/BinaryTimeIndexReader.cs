using System.Buffers.Binary;
using MessageBroker.Domain.Entities.CommitLog.Index;
using MessageBroker.Domain.Port.CommitLog.Index.Reader;

namespace MessageBroker.Inbound.CommitLog.Index.Reader;

public sealed class BinaryTimeIndexReader : ITimeIndexReader
{
    public TimeIndexEntry ReadFrom(Stream stream)
    {
        var entrySize = TimeIndexEntry.Size;
        Span<byte> buffer = stackalloc byte[entrySize];
        var bytesRead = stream.Read(buffer);

        if (bytesRead != entrySize)
        {
            throw new InvalidDataException(
                $"Failed to read complete time index entry. Expected {entrySize} bytes, got {bytesRead}");
        }

        var timestamp = BinaryPrimitives.ReadUInt64BigEndian(buffer[..8]);
        var relativeOffset = BinaryPrimitives.ReadUInt64BigEndian(buffer.Slice(8, 8));

        return new TimeIndexEntry(timestamp, relativeOffset);
    }
}