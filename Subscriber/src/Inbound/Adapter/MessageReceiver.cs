using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Subscriber.Domain.UseCase;

namespace Subscriber.Inbound.Adapter;

public class MessageReceiver<T>(
    Channel<byte[]> responseChannel,
    ProcessMessageUseCase<T> processMessageUseCase,
    TimeSpan pollInterval,
    CancellationToken cancellationToken,
    Action<ulong>? onOffsetReceived = null) where T : new()
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<MessageReceiver<T>>(LogSource.Subscriber);

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
                        var offset = await processMessageUseCase.ExecuteAsync(message);
                        onOffsetReceived?.Invoke(offset + 1);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
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
}