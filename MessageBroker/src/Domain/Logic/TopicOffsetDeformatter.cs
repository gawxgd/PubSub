using System.Text;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;

namespace MessageBroker.Domain.Logic;

public class TopicOffsetDeformatter
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<TopicOffsetDeformatter>(LogSource.MessageBroker);

    public TopicOffset? Deformat(ReadOnlyMemory<byte> message)
    {
        var requestString = Encoding.UTF8.GetString(message.Span);
        var parts = requestString.Split((char)TopicOffset.Separator);

        if (parts.Length != 2)
        {
            Logger.LogWarning($"Invalid request format: {requestString}, returning null");
            return null;
        }

        var topic = parts[0];
        if (!ulong.TryParse(parts[1], out var offset))
        {
            Logger.LogWarning($"Invalid offset in request: {parts[1]}, returning null");
            return null;
        }

        return new TopicOffset(topic, offset);
    }
}
