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
        AutoLoggerFactory.CreateLogger<ProcessMessageUseCase<T>>(LogSource.Subscriber);

    /// <summary>
    /// Process a framed batch message: [8-byte offset][4-byte length][payload]
    /// Returns the offset from the message for tracking.
    /// </summary>
    public async Task<ulong> ExecuteAsync(byte[] framedBatchData)
    {
        // Extract offset and payload from framed message
        var (offset, payload) = ParseFramedMessage(framedBatchData);

        Logger.LogDebug($"Processing message at offset {offset}, payload size: {payload.Length} bytes");

        var schema = await schemaRegistryClient.GetLatestSchemaByTopicAsync(topic);
        var deserializedMessages = await deserializeBatchUseCase.ExecuteAsync(payload, schema, schema);

        foreach (var message in deserializedMessages)
        {
            await messageHandler(message);
        }

        Logger.LogInfo($"Processed batch with {deserializedMessages.Count} messages at offset {offset}");
        return offset;
    }

    //ToDo move to seprate class
    private static (ulong offset, byte[] payload) ParseFramedMessage(byte[] framedData)
    {
        // Format: [8-byte offset][4-byte length][payload]
        if (framedData.Length < 12)
        {
            throw new InvalidDataException(
                $"Framed message too short: {framedData.Length} bytes, expected at least 12");
        }

        var offset = BitConverter.ToUInt64(framedData, 0);
        var payloadLength = BitConverter.ToInt32(framedData, 8);

        if (framedData.Length < 12 + payloadLength)
        {
            throw new InvalidDataException(
                $"Framed message incomplete: expected {12 + payloadLength} bytes, got {framedData.Length}");
        }

        var payload = new byte[payloadLength];
        Array.Copy(framedData, 12, payload, 0, payloadLength);

        return (offset, payload);
    }
}