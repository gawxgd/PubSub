using Publisher.Domain.Model;

namespace Publisher.Domain.Port;

public interface IAvroSerializer
{
    Task<byte[]> SerializeAsync<T>(T message, SchemaInfo schemaInfo);
}