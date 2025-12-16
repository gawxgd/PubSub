using Avro.IO;
using Avro.Reflect;
using Shared.Domain.Entities.SchemaRegistryClient;
using Subscriber.Domain;
using Subscriber.Domain.Exceptions;

namespace Subscriber.Outbound.Adapter;

public class AvroDeserializer<T> : IDeserializer<T> where T : new()
{
    public Task<T> DeserializeAsync(byte[] avroBytes, SchemaInfo writersSchema, SchemaInfo readersSchema)
    {
        try
        {
            var writerAvroSchema = Avro.Schema.Parse(writersSchema.SchemaJson.GetRawText());
            var readerAvroSchema = Avro.Schema.Parse(readersSchema.SchemaJson.GetRawText());

            var datumReader = new ReflectReader<T>(writerAvroSchema, readerAvroSchema);

            using var stream = new MemoryStream(avroBytes);
            var decoder = new BinaryDecoder(stream);

            var result = datumReader.Read(default!, decoder);

            return result == null
                ? throw new DeserializationException($"Avro deserialization returned null for type {typeof(T).Name}")
                : Task.FromResult(result);
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DeserializationException($"Failed to deserialize Avro data to type {typeof(T).Name}", ex);
        }
    }
}