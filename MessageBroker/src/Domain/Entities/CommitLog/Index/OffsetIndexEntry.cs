using System.Buffers.Binary;

namespace MessageBroker.Domain.Entities.CommitLog.Index;

public sealed record OffsetIndexEntry(ulong RelativeOffset, ulong FilePosition)
{
    public static int Size => sizeof(ulong) + sizeof(ulong);

    public static OffsetIndexEntry Read(FileStream fs)
    {
        Span<byte> buffer = stackalloc byte[Size];

        int read = fs.Read(buffer);
        if (read < Size)
            throw new EndOfStreamException("Unexpected end of offset index file");

        ulong relativeOffset = BinaryPrimitives.ReadUInt64LittleEndian(buffer[..8]);
        ulong filePosition   = BinaryPrimitives.ReadUInt64LittleEndian(buffer[8..]);

        return new OffsetIndexEntry(relativeOffset, filePosition);
    }
}