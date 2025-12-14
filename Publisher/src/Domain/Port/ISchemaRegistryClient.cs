using Publisher.Domain.Model;

namespace Publisher.Domain.Port;

public interface ISchemaRegistryClient
{
    public Task<SchemaInfo?> GetSchemaAsync(string topic);
}