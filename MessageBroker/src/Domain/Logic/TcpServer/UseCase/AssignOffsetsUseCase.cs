using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

public class AssignOffsetsUseCase
{
    private const int OffsetSize = sizeof(ulong);
    private const int LengthSize = sizeof(uint);

    private int _position;

    public ulong AssignOffsets(ulong baseOffset, ReadOnlyMemory<byte> batchMemory)
    {
        var batchBytes = MemoryMarshal.AsMemory(batchMemory).Span;

        AssignBatchBaseOffset(baseOffset, batchBytes);

        SkipBatchHeaders(batchBytes);

        baseOffset++;


        while (_position < batchBytes.Length)
        {
            AssignRecordOffset(baseOffset, batchBytes);
            SkipRecordHeaders(batchBytes);

            baseOffset++;
        }

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
        _position += LengthSize;

        var recordBytesLength =
            BinaryPrimitives.ReadUInt32LittleEndian(batchBytes.Slice(_position, LengthSize));
        _position += LengthSize;

        _position += (int)(batchLength - recordBytesLength);
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
}