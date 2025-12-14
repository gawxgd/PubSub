using SchemaRegistryClient;

namespace SchemaRegistryClient;

public interface ISchemaRegistryClient
{
    Task<SchemaInfo?> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default);
    Task<SchemaInfo?> GetLatestSchemaByTopicAsync(string topic, CancellationToken cancellationToken = default);
}