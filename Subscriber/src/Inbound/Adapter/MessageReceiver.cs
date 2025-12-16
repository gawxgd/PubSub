using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Shared.Domain.Entities.SchemaRegistryClient;
using Subscriber.Domain;
using Subscriber.Domain.UseCase;

namespace Subscriber.Inbound.Adapter;

public class MessageReceiver<T>(
    Channel<byte[]> responseChannel,
    DeserializeBatchUseCase<T> deserializeBatchUseCase,
    SchemaInfo writersSchema,
    SchemaInfo readersSchema,
    Func<T, Task>? messageHandler,
    TimeSpan pollInterval,
    CancellationToken cancellationToken) where T : new()
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<MessageReceiver<T>>(LogSource.MessageBroker);

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

    private async Task ProcessMessageAsync(byte[] message)
    {
        try
        {
            var deserializedMessages =
                await deserializeBatchUseCase.ExecuteAsync(message, writersSchema, readersSchema);

            foreach (var deserializedMessage in deserializedMessages)
            {
                if (messageHandler != null)
                {
                    await messageHandler(deserializedMessage);
                }
            }

            Logger.LogInfo($"Processed batch with {deserializedMessages.Count} events");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Error handling message: {ex.Message}", ex);
        }
    }
}