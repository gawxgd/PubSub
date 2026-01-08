using Shared.Domain.Entities.SchemaRegistryClient;
using Shared.Domain.Exceptions.SchemaRegistryClient;

namespace Shared.Domain.Port.SchemaRegistryClient;

public interface ISchemaRegistryClient
{
    /// <exception cref="SchemaNotFoundException">
    /// Thrown when no schema exists for the given topic.
    /// </exception>
    /// <exception cref="SchemaRegistryClientException">
    /// Thrown when communication with the schema registry fails.
    /// </exception>
    Task<SchemaInfo> GetLatestSchemaByTopicAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// Registers a schema for a topic. Returns existing ID if same schema content exists for this topic.
    /// </summary>
    /// <exception cref="SchemaRegistryClientException">
    /// Thrown when communication with the schema registry fails.
    /// </exception>
    Task<SchemaInfo> RegisterSchemaAsync(string topic, string schemaJson, CancellationToken cancellationToken = default);

    /// <exception cref="SchemaNotFoundException">
    /// Thrown when no schema exists for the given ID.
    /// </exception>
    /// <exception cref="SchemaRegistryClientException">
    /// Thrown when communication with the schema registry fails.
    /// </exception>
    Task<SchemaInfo> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default);
}

