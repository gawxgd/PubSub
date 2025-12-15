using SchemaRegistryClient;

namespace Subscriber.Domain;

public interface IDeserializer<T> where T : new()
{
    Task<T?> DeserializeAsync(byte[] avroBytes, SchemaInfo writersSchema, SchemaInfo readersSchema);
}