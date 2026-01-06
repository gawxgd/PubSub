using System;
using System.Text;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;

namespace MessageBroker.Domain.Logic;

public class MessageWithTopicDeformatter
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<MessageWithTopicDeformatter>(LogSource.MessageBroker);

    public MessageWithTopic? Deformat(ReadOnlyMemory<byte> message)
    {
        var span = message.Span;
        
        if (message.Length == 0)
        {
            Logger.LogWarning("Invalid message format: empty message");
            return null;
        }
        
        var separatorIndex = span.IndexOf(MessageWithTopic.Separator);
        if (separatorIndex == -1)
        {
            Logger.LogWarning($"Invalid message format: missing topic separator. Message length: {message.Length}, first 50 bytes: {Convert.ToHexString(span.Slice(0, Math.Min(50, message.Length)))}");
            return null;
        }

        if (separatorIndex == 0)
        {
            Logger.LogWarning("Invalid message format: separator at position 0 (no topic)");
            return null;
        }

        var topic = Encoding.UTF8.GetString(span.Slice(0, separatorIndex));
        var payloadLength = message.Length - separatorIndex - 1;
        
        Logger.LogInfo($"🔍 Deformatting: messageLength={message.Length}, separatorIndex={separatorIndex}, topic='{topic}', payloadLength={payloadLength}");
        
        if (payloadLength <= 0)
        {
            Logger.LogWarning($"Invalid message format: no payload after separator. Message length: {message.Length}, separator at: {separatorIndex}, topic='{topic}'");
            Logger.LogWarning($"Full message hex: {Convert.ToHexString(span)}");
            return null;
        }
        
        if (payloadLength < 38)
        {
            Logger.LogWarning($"Payload too small: {payloadLength} bytes (expected at least 38). Topic: '{topic}'");
            Logger.LogWarning($"Message structure: topic='{topic}' ({separatorIndex} bytes) + separator (1 byte) + payload ({payloadLength} bytes)");
            Logger.LogWarning($"Payload hex: {Convert.ToHexString(span.Slice(separatorIndex + 1, Math.Min(payloadLength, 50)))}");
        }
        
        var payload = span.Slice(separatorIndex + 1).ToArray();

        Logger.LogInfo($"Deformatted message: topic='{topic}', payload={payloadLength} bytes");
        return new MessageWithTopic(topic, payload);
    }
}

