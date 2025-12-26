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
        
        var separatorIndex = span.IndexOf(MessageWithTopic.Separator);
        if (separatorIndex == -1)
        {
            Logger.LogWarning("Invalid message format: missing topic separator");
            return null;
        }

        var topic = Encoding.UTF8.GetString(span.Slice(0, separatorIndex));
        var payload = span.Slice(separatorIndex + 1).ToArray();

        return new MessageWithTopic(topic, payload);
    }
}

