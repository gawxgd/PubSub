using Shared.Domain.Entities.SchemaRegistryClient;

namespace Shared.Domain.Port.SchemaRegistryClient;

public interface ISchemaCache
{
    bool TryGet(int schemaId, out SchemaInfo schema);
    bool TryGet(string topic, out SchemaInfo schema);
    void AddToCache(int schemaId, SchemaInfo schema);
    void AddToCache(string topic, SchemaInfo schema);
}