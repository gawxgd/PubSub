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

    private ulong _lastOffset = 0;

    public async Task SendRequestAsync(ulong offset)
    {
        var topicOffset = new TopicOffset(topic, offset);
        var requestBytes = _formatter.Format(topicOffset);
        var framedMessage = _messageFramer.FrameMessage(requestBytes);

        await requestChannel.Writer.WriteAsync(framedMessage, cancellationToken);
        Logger.LogInfo(
            $"Sent request for topic: {topic}, offset: {offset} (framed: {framedMessage.Length} bytes)");
    }

    public void UpdateOffset(ulong offset)
    {
        var oldOffset = _lastOffset;
        _lastOffset = offset;
        Logger.LogInfo($"Updated last offset from {oldOffset} to: {offset}");
    }
}