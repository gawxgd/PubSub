using Shared.Domain.Entities.SchemaRegistryClient;

namespace Publisher.Domain.Port;

public interface IAvroSerializer<in T>
{
    byte[] Serialize(T message, SchemaInfo schemaInfo);
}
