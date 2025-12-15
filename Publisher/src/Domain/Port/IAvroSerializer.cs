using SchemaRegistryClient;

namespace Publisher.Domain.Port;

public interface IAvroSerializer<in T>
{
    Task<byte[]> SerializeAsync(T message, SchemaInfo schemaInfo);
}