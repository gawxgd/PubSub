using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Logic;
using MessageBroker.Domain.Port;
using MessageBroker.Outbound.Adapter;

namespace Subscriber.Outbound.Adapter;

public class RequestSender(
    Channel<byte[]> requestChannel,
    string topic,
    CancellationToken cancellationToken)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<RequestSender>(LogSource.Subscriber);

    private readonly IMessageFramer _messageFramer = new MessageFramer();
    private readonly TopicOffsetFormatter _formatter = new();

    public async Task SendRequestAsync(ulong offset)
    {
        var topicOffset = new TopicOffset(topic, offset);
        var requestBytes = _formatter.Format(topicOffset);
        var framedMessage = _messageFramer.FrameMessage(requestBytes);

        await requestChannel.Writer.WriteAsync(framedMessage, cancellationToken);
        //Logger.LogDebug(
        //    $"Sent request for topic: {topic}, offset: {offset} (framed: {framedMessage.Length} bytes)");
    }
}
