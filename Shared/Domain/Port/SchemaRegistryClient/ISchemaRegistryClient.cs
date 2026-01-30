using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Exceptions.SchemaRegistryClient;

namespace Shared.Domain.Port.SchemaRegistryClient;

public interface ISchemaRegistryClient
{

    Task<SchemaInfo> GetLatestSchemaByTopicAsync(string topic, CancellationToken cancellationToken = default);

    Task<SchemaInfo> RegisterSchemaAsync(string topic, string schemaJson, CancellationToken cancellationToken = default);

    Task<SchemaInfo> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default);
}

