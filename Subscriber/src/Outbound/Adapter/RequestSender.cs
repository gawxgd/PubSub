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
    TimeSpan pollInterval,
    CancellationToken cancellationToken)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<RequestSender>(LogSource.Subscriber);

    private readonly IMessageFramer _messageFramer = new MessageFramer();
    private readonly TopicOffsetFormatter _formatter = new();

    private ulong _lastOffset = 0;

    public async Task StartSendingAsync()
    {
        Logger.LogInfo($"Starting request sender for topic: {topic}");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var topicOffset = new TopicOffset(topic, _lastOffset);
                var requestBytes = _formatter.Format(topicOffset);

                var framedMessage = _messageFramer.FrameMessage(requestBytes);

                await requestChannel.Writer.WriteAsync(framedMessage, cancellationToken);
                Logger.LogDebug(
                    $"Sent request for topic: {topic}, offset: {_lastOffset} (framed: {framedMessage.Length} bytes)");

                // Wait before sending next request
                await Task.Delay(pollInterval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                Logger.LogInfo("Request sender cancelled");
                break;
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error in request sender: {ex.Message}");
                // Continue sending even if there's an error
                try
                {
                    await Task.Delay(pollInterval, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        Logger.LogInfo("Request sender stopped");
    }

    public void UpdateOffset(ulong offset)
    {
        _lastOffset = offset;
        Logger.LogDebug($"Updated last offset to: {offset}");
    }
}