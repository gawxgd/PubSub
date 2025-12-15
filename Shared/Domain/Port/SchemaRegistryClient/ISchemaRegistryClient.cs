namespace Shared.Domain.Port.SchemaRegistry;

public interface ISchemaRegistryClient
{
    //ToDo throw on schema not found
    Task<SchemaInfo?> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default);
    Task<SchemaInfo?> GetLatestSchemaByTopicAsync(string topic, CancellationToken cancellationToken = default);
}