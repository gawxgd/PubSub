using Subscriber.Domain.Model;

namespace Subscriber.Domain;

public interface ISchemaRegistryClient
{
    public Task<SchemaInfo> GetSchemaByIdAsync(int id);
    public Task<SchemaInfo> GetLatestSchemaByTopicAsync(string topic);
}