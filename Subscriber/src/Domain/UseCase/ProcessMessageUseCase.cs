using System.IO;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using Shared.Domain.Port.SchemaRegistryClient;

namespace Subscriber.Domain.UseCase;

public class ProcessMessageUseCase<T>(
    DeserializeBatchUseCase<T> deserializeBatchUseCase,
    ISchemaRegistryClient schemaRegistryClient,
    Func<T, Task> messageHandler,
    string topic,
    ILogRecordBatchReader batchReader) where T : new()
{
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<ProcessMessageUseCase<T>>(LogSource.Subscriber);

    public async Task<ulong> ExecuteAsync(byte[] batchBytes)
    {
        using var stream = new MemoryStream(batchBytes);
        var batch = batchReader.ReadBatch(stream);

        Logger.LogDebug(
            $"Processing batch at baseOffset {batch.BaseOffset}, lastOffset {batch.LastOffset}, {batch.Records.Count} records");

        var schema = await schemaRegistryClient.GetLatestSchemaByTopicAsync(topic);
        var deserializedMessages = await deserializeBatchUseCase.ExecuteAsync(batchBytes, schema, schema);

        foreach (var message in deserializedMessages)
        {
            await messageHandler(message);
        }

        Logger.LogInfo(
            $"Processed batch with {deserializedMessages.Count} messages, baseOffset={batch.BaseOffset}, lastOffset={batch.LastOffset}");
        return batch.LastOffset;
    }
    
    public (ulong baseOffset, ulong lastOffset) GetBatchOffsets(byte[] batchBytes)
    {
        using var stream = new MemoryStream(batchBytes);
        var batch = batchReader.ReadBatch(stream);
        return (batch.BaseOffset, batch.LastOffset);
    }
}