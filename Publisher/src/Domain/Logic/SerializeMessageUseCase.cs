using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
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
    private static readonly IAutoLogger Logger =
        AutoLoggerFactory.CreateLogger<SerializeMessageUseCase<T>>(LogSource.Publisher);

    private SchemaInfo? _cachedSchema;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private readonly AvroSchemaGenerator _avroSchemaGenerator = new();

    private async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await _schemaLock.WaitAsync(cancellationToken);
        try
        {
            if (_cachedSchema != null) return;

            Logger.LogDebug($"Initializing schema for topic '{topic}'...");

            var schemaJson = _avroSchemaGenerator.GenerateSchemaJson<T>();
            _cachedSchema = await schemaRegistryClient.RegisterSchemaAsync(topic, schemaJson, cancellationToken);
            Logger.LogInfo($"Registered new schema for topic '{topic}' with ID: {_cachedSchema.SchemaId}");
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
