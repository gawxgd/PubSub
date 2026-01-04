using System.Buffers.Binary;
using System.IO.Pipelines;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port;

namespace MessageBroker.Outbound.Adapter;

public class MessageFramer : IMessageFramer
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<MessageFramer>(LogSource.MessageBroker);

    private const int LengthFieldSize = 4;

    public byte[] FrameMessage(byte[] message)
    {
        var framedMessage = new byte[LengthFieldSize + message.Length];
        // Use BinaryPrimitives to ensure LittleEndian encoding (consistent across systems)
        BinaryPrimitives.WriteInt32LittleEndian(framedMessage.AsSpan(0, LengthFieldSize), message.Length);
        message.CopyTo(framedMessage, LengthFieldSize);

        Logger.LogDebug($"Framed message: length={message.Length} bytes");
        return framedMessage;
    }
}