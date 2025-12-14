using Subscriber.Domain.Model;

namespace Subscriber.Domain;

public interface IDeserializer
{
    Task<object?> DeserializeAsync(byte[] avroBytes, SchemaInfo writersSchema, SchemaInfo readersSchema);
}