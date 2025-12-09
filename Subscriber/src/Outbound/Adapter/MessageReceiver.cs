using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace Subscriber.Outbound.Adapter;

public class MessageReceiver(
    Channel<byte[]> responseChannel,
    MessageValidator messageValidator,
    Func<string, Task>? messageHandler,
    TimeSpan pollInterval,
    CancellationToken cancellationToken)
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<MessageReceiver>(LogSource.MessageBroker);

    public async Task StartReceivingAsync()
    {
        Logger.LogInfo("Starting message receiver");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (await responseChannel.Reader.WaitToReadAsync(cancellationToken))
                {
                    while (responseChannel.Reader.TryRead(out var message))
                    {
                        await ProcessMessageAsync(message);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.LogError($"Error while processing message: {ex.Message}", ex);
            }

            try
            {
                await Task.Delay(pollInterval, cancellationToken);
            }
            catch (TaskCanceledException)
            {
                Logger.LogWarning("Message processing task cancelled");
            }
        }

        Logger.LogInfo("Message receiver stopped");
    }

    private async Task ProcessMessageAsync(byte[] message)
    {
        var validationResult = messageValidator.Validate(message);

        if (!validationResult.IsValid)
        {
            return;
        }

        try
        {
            if (messageHandler != null)
            {
                await messageHandler(validationResult.Payload!);
            }
            Logger.LogInfo($"Received message: {validationResult.Payload}");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Error handling message: {ex.Message}", ex);
        }
    }
}

