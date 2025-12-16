using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using Shared.Domain.Entities.SchemaRegistryClient;

namespace Subscriber.Domain.UseCase;

public class DeserializeBatchUseCase<T>(
    ILogRecordBatchReader batchReader,
    IDeserializer<T> deserializer)
{
    public async Task<IReadOnlyList<T>> ExecuteAsync(byte[] batchData, SchemaInfo writersSchema,
        SchemaInfo readersSchema)
    {
        using var stream = new MemoryStream(batchData);
        var batch = batchReader.ReadBatch(stream);

        var results = new List<T>();

        foreach (var record in batch.Records)
        {
            var deserialized = await deserializer.DeserializeAsync(
                record.Payload.ToArray(),
                writersSchema,
                readersSchema);

            results.Add(deserialized);
        }

        return results;
    }
}