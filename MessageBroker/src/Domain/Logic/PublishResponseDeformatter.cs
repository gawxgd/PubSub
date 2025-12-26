using System.Buffers.Binary;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Enums;

namespace MessageBroker.Domain.Logic;

public class PublishResponseDeformatter
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<PublishResponseDeformatter>(LogSource.MessageBroker);

    public PublishResponse? Deformat(ReadOnlyMemory<byte> message)
    {
        if (message.Length < sizeof(ulong) + sizeof(short))
        {
            Logger.LogWarning($"Invalid response format: message too short ({message.Length} bytes)");
            return null;
        }

        var span = message.Span;
        var baseOffset = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(0, sizeof(ulong)));
        var errorCode = (ErrorCode)BinaryPrimitives.ReadInt16LittleEndian(span.Slice(sizeof(ulong), sizeof(short)));

        return new PublishResponse(baseOffset, errorCode);
    }
}

