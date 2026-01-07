using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.IO.Hashing;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class AssignOffsetsUseCase
{
    private const int OffsetSize = sizeof(ulong);
    private const int LengthSize = sizeof(uint);
    private const int MagicNumberSize = sizeof(byte);
    private const int LastOffsetPosition = OffsetSize + LengthSize;
    private const int RecordBytesLengthPosition = OffsetSize + LengthSize + OffsetSize;
    private const int CrcPosition = OffsetSize + LengthSize + OffsetSize + LengthSize + MagicNumberSize;
    private const int CrcSize = sizeof(uint);
    private const int TimestampSize = sizeof(ulong);

    private const int CompressedFlagSize = sizeof(byte);

    private const int RecordBytesStartPosition =
        OffsetSize + LengthSize + OffsetSize + LengthSize + MagicNumberSize + CrcSize + CompressedFlagSize +
        TimestampSize;

    private int _position;

    public ulong AssignOffsets(ulong baseOffset, ReadOnlyMemory<byte> batchMemory)
    {
        _position = 0;

        var batchBytes = MemoryMarshal.AsMemory(batchMemory).Span;

        VerifyCrc(batchBytes);

        AssignBatchBaseOffset(baseOffset, batchBytes);

        SkipBatchHeaders(batchBytes);

        while (_position < batchBytes.Length)
        {
            AssignRecordOffset(baseOffset, batchBytes);
            SkipRecordHeaders(batchBytes);

            baseOffset++;
        }

        var lastOffset = baseOffset - 1;
        AssignBatchLastOffset(lastOffset, batchBytes);

        RecalculateAndUpdateCrc(batchBytes);

        return baseOffset;
    }

    private void AssignBatchBaseOffset(ulong baseOffset, Span<byte> batchBytes)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(
            batchBytes.Slice(_position, OffsetSize),
            baseOffset
        );

        _position += OffsetSize;
    }

    private void SkipBatchHeaders(Span<byte> batchBytes)
    {
        var batchLength = BinaryPrimitives.ReadUInt32LittleEndian(batchBytes.Slice(_position, LengthSize));
        _position += LengthSize + OffsetSize;

        var recordBytesLength =
            BinaryPrimitives.ReadUInt32LittleEndian(batchBytes.Slice(_position, LengthSize));
        _position += LengthSize;

        _position += (int)(batchLength - recordBytesLength);
    }

    private void AssignBatchLastOffset(ulong lastOffset, Span<byte> batchBytes)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(
            batchBytes.Slice(LastOffsetPosition, OffsetSize),
            lastOffset
        );
    }


    private void AssignRecordOffset(ulong currentOffset, Span<byte> batchBytes)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(
            batchBytes.Slice(_position, OffsetSize),
            currentOffset
        );

        _position += OffsetSize;
    }

    private void SkipRecordHeaders(Span<byte> batchBytes)
    {
        var recordLength = BinaryPrimitives.ReadUInt32LittleEndian(batchBytes.Slice(_position, LengthSize));
        _position += LengthSize;
        _position += (int)recordLength;
    }

    private void VerifyCrc(Span<byte> batchBytes)
    {
        var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(batchBytes.Slice(CrcPosition, CrcSize));

        var recordBytesLength =
            BinaryPrimitives.ReadUInt32LittleEndian(batchBytes.Slice(RecordBytesLengthPosition, LengthSize));

        var recordBytes = batchBytes.Slice(RecordBytesStartPosition, (int)recordBytesLength);

        var computedCrc = Crc32.HashToUInt32(recordBytes);

        if (computedCrc != storedCrc)
        {
            throw new InvalidDataException($"Batch CRC mismatch: expected {storedCrc}, got {computedCrc}");
        }
    }


    private void RecalculateAndUpdateCrc(Span<byte> batchBytes)
    {
        var recordBytesLength =
            BinaryPrimitives.ReadUInt32LittleEndian(batchBytes.Slice(RecordBytesLengthPosition, LengthSize));

        var recordBytes = batchBytes.Slice(RecordBytesStartPosition, (int)recordBytesLength);

        var newCrc = Crc32.HashToUInt32(recordBytes);

        BinaryPrimitives.WriteUInt32LittleEndian(
            batchBytes.Slice(CrcPosition, LengthSize),
            newCrc
        );
    }
}