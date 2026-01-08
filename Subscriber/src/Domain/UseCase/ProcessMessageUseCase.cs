using System.IO;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using Shared.Domain.Avro;
using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Exceptions.SchemaRegistryClient;
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

    private SchemaInfo? _cachedReaderSchema;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);

    private async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await _schemaLock.WaitAsync(cancellationToken);
        try
        {
            if (_cachedReaderSchema != null) return;

            try
            {
                _cachedReaderSchema = await schemaRegistryClient.GetLatestSchemaByTopicAsync(topic, cancellationToken);
                Logger.LogInfo($"Found existing schema for topic '{topic}' with ID: {_cachedReaderSchema.SchemaId}");
                return;
            }
            catch (SchemaNotFoundException e)
            {
                Logger.LogDebug($"No existing schema found for topic '{topic}', generating new schema...", e);
            }

            var schemaJson = AvroSchemaGenerator.GenerateSchemaJson<T>();
            _cachedReaderSchema = await schemaRegistryClient.RegisterSchemaAsync(topic, schemaJson, cancellationToken);
            Logger.LogInfo($"Registered new schema for topic '{topic}' with ID: {_cachedReaderSchema.SchemaId}");
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    public async Task<ulong> ExecuteAsync(byte[] batchBytes)
    {
        using var stream = new MemoryStream(batchBytes);
        var batch = batchReader.ReadBatch(stream);

        Logger.LogDebug(
            $"Processing batch at baseOffset {batch.BaseOffset}, lastOffset {batch.LastOffset}, {batch.Records.Count} records");

        if (_cachedReaderSchema == null)
        {
            await InitializeSchemaAsync();
        }

        var deserializedMessages = await deserializeBatchUseCase.ExecuteAsync(batchBytes, _cachedReaderSchema!);

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