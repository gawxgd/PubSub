using System.Text;
using MessageBroker.Domain.Entities;

namespace MessageBroker.Domain.Logic;

public class MessageWithTopicFormatter
{
    public byte[] Format(MessageWithTopic message)
    {
        var topicBytes = Encoding.UTF8.GetBytes(message.Topic);
        var formattedMessage = new byte[topicBytes.Length + 1 + message.Payload.Length];

        topicBytes.CopyTo(formattedMessage, 0);
        formattedMessage[topicBytes.Length] = MessageWithTopic.Separator;
        message.Payload.CopyTo(formattedMessage, topicBytes.Length + 1);

        return formattedMessage;
    }
}