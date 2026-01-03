using System.Text;
using MessageBroker.Domain.Entities;

namespace MessageBroker.Domain.Logic;

public class TopicOffsetFormatter
{
    public byte[] Format(TopicOffset topicOffset)
    {
        var topicBytes = Encoding.UTF8.GetBytes(topicOffset.Topic);
        var offsetString = topicOffset.Offset.ToString();
        var offsetBytes = Encoding.UTF8.GetBytes(offsetString);
        var formattedMessage = new byte[topicBytes.Length + 1 + offsetBytes.Length];

        topicBytes.CopyTo(formattedMessage, 0);
        formattedMessage[topicBytes.Length] = TopicOffset.Separator;
        offsetBytes.CopyTo(formattedMessage, topicBytes.Length + 1);

        return formattedMessage;
    }
}

