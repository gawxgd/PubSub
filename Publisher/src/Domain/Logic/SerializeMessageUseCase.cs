using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Publisher.Domain.Port;
using Shared.Domain.Avro;
using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Exceptions.SchemaRegistryClient;
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

    private async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await _schemaLock.WaitAsync(cancellationToken);
        try
        {
            if (_cachedSchema != null) return;

            Logger.LogDebug($"Initializing schema for topic '{topic}'...");

            try
            {
                _cachedSchema = await schemaRegistryClient.GetLatestSchemaByTopicAsync(topic, cancellationToken);
                Logger.LogInfo($"Found existing schema for topic '{topic}' with ID: {_cachedSchema.SchemaId}");
                return;
            }
            catch (SchemaNotFoundException e)
            {
                Logger.LogDebug($"No existing schema found for topic '{topic}', generating new schema...", e);
            }

            var schemaJson = AvroSchemaGenerator.GenerateSchemaJson<T>();
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
