using SchemaRegistryClient;

namespace Publisher.Domain.Port;

public interface IAvroSerializer<T>
{
    Task<byte[]> SerializeAsync(T message, SchemaInfo schemaInfo);
}