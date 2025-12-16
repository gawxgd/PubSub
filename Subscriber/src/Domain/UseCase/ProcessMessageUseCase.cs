using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Shared.Domain.Port.SchemaRegistryClient;

namespace Subscriber.Domain.UseCase;

public class ProcessMessageUseCase<T>(
    DeserializeBatchUseCase<T> deserializeBatchUseCase,
    ISchemaRegistryClient schemaRegistryClient,
    Func<T, Task> messageHandler,
    string topic) where T : new()
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessMessageUseCase<T>>(LogSource.MessageBroker);

    public async Task ExecuteAsync(byte[] batchData)
    {
        var schema = await schemaRegistryClient.GetLatestSchemaByTopicAsync(topic);
        var deserializedMessages = await deserializeBatchUseCase.ExecuteAsync(batchData, schema, schema);

        foreach (var message in deserializedMessages)
        {
            await messageHandler(message);
        }

        Logger.LogInfo($"Processed batch with {deserializedMessages.Count} messages");
    }
}

