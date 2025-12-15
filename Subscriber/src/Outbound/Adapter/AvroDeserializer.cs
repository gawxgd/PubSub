using Avro;
using Avro.Generic;
using Avro.IO;
using Microsoft.Extensions.Logging;
using Shared.Domain.Entities.SchemaRegistryClient;
using Subscriber.Domain;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace Subscriber.Outbound.Adapter;

public class AvroDeserializer<T> : IDeserializer<T> where T : new()
{
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<TcpSubscriber<T>>(LogSource.MessageBroker);
    
    public Task<T?> DeserializeAsync(byte[] avroBytes, SchemaInfo writersSchema, SchemaInfo readersSchema)
    {
            var writerAvroSchema = Avro.Schema.Parse(writersSchema.Json);
            var readerAvroSchema = Avro.Schema.Parse(readersSchema.Json);
        
            var datumReader = new GenericDatumReader<GenericRecord>(writerAvroSchema, readerAvroSchema);

            using var stream = new MemoryStream(avroBytes);
            var decoder = new BinaryDecoder(stream);
        
            var record = datumReader.Read(null, decoder);
        
            return Task.FromResult(Map(record));
    }
    private static T Map(GenericRecord record)
    {
        var result = new T();
        var type = typeof(T); // metadata of type T available in runtime

        foreach (var field in record.Schema.Fields)
        {
            var prop = type.GetProperty(field.Name);
            if (prop == null || !prop.CanWrite)
                continue;

            prop.SetValue(result, record[field.Name]);
        }

        return result;
    }
}