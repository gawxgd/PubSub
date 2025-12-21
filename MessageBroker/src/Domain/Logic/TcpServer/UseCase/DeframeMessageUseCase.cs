using System.Buffers;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace MessageBroker.Domain.Logic.TcpServer.UseCase;

/// <summary>
/// Deframes messages from publisher-broker protocol.
/// Format: [4-byte length][topic][:][batch bytes]
/// </summary>
/// ToDo change
public class DeframeMessageUseCase
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<DeframeMessageUseCase>(LogSource.MessageBroker);
    
    public bool TryReadFramedMessage(ref ReadOnlySequence<byte> buffer, out byte[] message)
    {
        message = Array.Empty<byte>();

        // Need at least 4 bytes for length prefix
        if (buffer.Length < 4)
            return false;

        // Read length prefix
        Span<byte> lengthSpan = stackalloc byte[4];
        buffer.Slice(0, 4).CopyTo(lengthSpan);
        var messageLength = BitConverter.ToInt32(lengthSpan);

        // Check if we have the full message
        if (buffer.Length < 4 + messageLength)
            return false;

        // Extract message (without length prefix)
        message = buffer.Slice(4, messageLength).ToArray();
        buffer = buffer.Slice(4 + messageLength);

        Logger.LogDebug($"Deframed message: length={messageLength} bytes");
        return true;
    }
}

