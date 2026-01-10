using System.Buffers.Binary;
using MessageBroker.Domain.Entities;

namespace MessageBroker.Domain.Logic;

public class PublishResponseFormatter
{
    public byte[] Format(PublishResponse response)
    {
        var formattedMessage = new byte[sizeof(ulong) + sizeof(short)];

        BinaryPrimitives.WriteUInt64LittleEndian(formattedMessage.AsSpan(0, sizeof(ulong)), response.BaseOffset);
        BinaryPrimitives.WriteInt16LittleEndian(formattedMessage.AsSpan(sizeof(ulong), sizeof(short)), (short)response.ErrorCode);

        return formattedMessage;
    }
}

