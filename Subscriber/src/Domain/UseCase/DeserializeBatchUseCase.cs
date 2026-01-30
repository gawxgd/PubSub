using System.Buffers.Binary;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Port.SchemaRegistryClient;

namespace Subscriber.Domain.UseCase;

public class DeserializeBatchUseCase<T>(
    ILogRecordBatchReader batchReader,
    IDeserializer<T> deserializer,
    ISchemaRegistryClient schemaRegistryClient) where T : new()
{
    private const int SchemaIdPrefixLength = 4;
    
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<DeserializeBatchUseCase<T>>(LogSource.Subscriber);
    
    public async Task<IReadOnlyList<T>> ExecuteAsync(byte[] batchData, SchemaInfo readersSchema)
    {
        using var stream = new MemoryStream(batchData);
        var batch = batchReader.ReadBatch(stream);

        var results = new List<T>();
        
        var writerSchemaCache = new Dictionary<int, SchemaInfo>();

        foreach (var record in batch.Records)
        {
            var payload = record.Payload.ToArray();
            
            if (payload.Length < SchemaIdPrefixLength)
            {
                Logger.LogWarning($"Record payload too short to contain schemaId prefix: {payload.Length} bytes");
                continue;
            }
            
            var schemaId = BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(0, SchemaIdPrefixLength));
            
            if (!writerSchemaCache.TryGetValue(schemaId, out var writersSchema))
            {
                Logger.LogDebug($"Fetching writer schema for schemaId: {schemaId}");
                writersSchema = await schemaRegistryClient.GetSchemaByIdAsync(schemaId);
                writerSchemaCache[schemaId] = writersSchema;
            }

            var deserialized = await deserializer.DeserializeAsync(
                payload,
                writersSchema,
                readersSchema);

            results.Add(deserialized);
        }

        return results;
    }
}
