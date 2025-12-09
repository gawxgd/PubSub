using System.Text;
using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace Subscriber.Outbound.Adapter;

public class RequestSender(
    Channel<byte[]> requestChannel,
    string topic,
    TimeSpan pollInterval,
    CancellationToken cancellationToken)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<RequestSender>(LogSource.MessageBroker);

    private ulong _lastOffset = 0; // TODO: track offset from received messages

    public async Task StartSendingAsync()
    {
        Logger.LogInfo($"Starting request sender for topic: {topic}");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Create request message: "topic:offset" format
                // TODO: Update format when ProcessSubscriberRequestUseCase implements proper parsing
                var requestMessage = $"{topic}:{_lastOffset}\n";
                var requestBytes = Encoding.UTF8.GetBytes(requestMessage);

                await requestChannel.Writer.WriteAsync(requestBytes, cancellationToken);
                Logger.LogDebug($"Sent automatic request for topic: {topic}, offset: {_lastOffset}");

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
                Logger.LogError($"Error in request sender: {ex.Message}", ex);
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
