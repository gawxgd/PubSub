using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Linq;
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
            Logger.LogDebug($"Not enough data for length field: {buffer.Length} < {LengthFieldSize}");
            return false;
        }

        Span<byte> lengthSpan = stackalloc byte[LengthFieldSize];
        buffer.Slice(0, LengthFieldSize).CopyTo(lengthSpan);
        
        // Debug: log the length field bytes
        Logger.LogDebug($"Length field bytes (hex): {Convert.ToHexString(lengthSpan)}");
        
        // Use BinaryPrimitives to match MessageFramer's LittleEndian encoding
        var messageLength = BinaryPrimitives.ReadInt32LittleEndian(lengthSpan);
        
        Logger.LogInfo($"📏 Read message length: {messageLength} bytes (from hex: {Convert.ToHexString(lengthSpan)})");

        if (messageLength < 0 || messageLength > 10 * 1024 * 1024) // Max 10MB
        {
            Logger.LogWarning($"❌ Invalid message length: {messageLength} bytes (hex: {Convert.ToHexString(lengthSpan)})");
            // Try to skip invalid length and continue
            if (buffer.Length >= LengthFieldSize)
            {
                buffer = buffer.Slice(LengthFieldSize);
            }
            return false;
        }

        var requiredLength = LengthFieldSize + messageLength;
        if (buffer.Length < requiredLength)
        {
            Logger.LogDebug($"Not enough data for full message: {buffer.Length} < {requiredLength} (need {messageLength} more bytes)");
            return false;
        }

        message = buffer.Slice(LengthFieldSize, messageLength).ToArray();
        buffer = buffer.Slice(requiredLength);

        Logger.LogInfo($"✅ Deframed message: length={messageLength} bytes (framed: {requiredLength} bytes total)");
        if (messageLength < 50)
        {
            Logger.LogDebug($"Message hex (full {messageLength} bytes): {Convert.ToHexString(message)}");
        }
        else
        {
            Logger.LogDebug($"Message hex (first 50 bytes): {Convert.ToHexString(message.Take(50).ToArray())}");
        }
        return true;
    }
}