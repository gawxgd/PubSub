using Avro.Generic;
using Avro.IO;
using Subscriber.Domain.Model;
using Subscriber.Domain;

namespace Subscriber.Outbound.Adapter;

public class AvroDeserializer : IDeserializer
{
    public Task<object?> DeserializeAsync(byte[] avroBytes, SchemaInfo writersSchema, SchemaInfo readersSchema)
    {
        try
        {
            var writerAvroSchema = Avro.Schema.Parse(writersSchema.Json);
            var readerAvroSchema = Avro.Schema.Parse(readersSchema.Json);
        
            var datumReader = new GenericDatumReader<GenericRecord>(writerAvroSchema, readerAvroSchema);

            using var stream = new MemoryStream(avroBytes);
            var decoder = new BinaryDecoder(stream);
        
            var record = datumReader.Read(null, decoder);
        
            return Task.FromResult<object?>(record);
        }
        catch (Exception)
        {
            return Task.FromResult<object?>(null);
        }
    }
}