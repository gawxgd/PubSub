using Publisher.Domain.Port;
using Shared.Domain.Avro;
using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Port.SchemaRegistryClient;

namespace Publisher.Domain.Logic;

public class SerializeMessageUseCase<T>(
    IAvroSerializer<T> serializer,
    ISchemaRegistryClient schemaRegistryClient,
    string topic)
{
    private SchemaInfo? _cachedSchema;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);

    private async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await _schemaLock.WaitAsync(cancellationToken);
        try
        {
            if (_cachedSchema != null) return;

            var schemaJson = AvroSchemaGenerator.GenerateSchemaJson<T>();
            _cachedSchema = await schemaRegistryClient.RegisterSchemaAsync(topic, schemaJson, cancellationToken);
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    public async Task<byte[]> Serialize(T message)
    {
        if (_cachedSchema == null)
        {
            await InitializeSchemaAsync();
        }

        return serializer.Serialize(message, _cachedSchema!);
    }
}