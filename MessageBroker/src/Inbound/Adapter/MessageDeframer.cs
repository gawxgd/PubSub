using System.Buffers;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port;

namespace MessageBroker.Inbound.Adapter;

public class MessageDeframer : IMessageDeframer
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<MessageDeframer>(LogSource.MessageBroker); //ToDo correct

    private const int LengthFieldSize = 4;

    public bool TryReadFramedMessage(ref ReadOnlySequence<byte> buffer, out byte[] message)
    {
        message = [];

        if (buffer.Length < LengthFieldSize)
        {
            return false;
        }

        Span<byte> lengthSpan = stackalloc byte[LengthFieldSize];
        buffer.Slice(0, LengthFieldSize).CopyTo(lengthSpan);
        var messageLength = BitConverter.ToInt32(lengthSpan);

        if (buffer.Length < LengthFieldSize + messageLength)
        {
            return false;
        }

        message = buffer.Slice(LengthFieldSize, messageLength).ToArray();
        buffer = buffer.Slice(LengthFieldSize + messageLength);

        Logger.LogDebug($"Deframed message: length={messageLength} bytes");
        return true;
    }
}