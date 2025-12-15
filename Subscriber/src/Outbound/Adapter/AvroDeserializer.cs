using Avro.Generic;
using Avro.IO;
using Microsoft.Extensions.Logging;
using SchemaRegistryClient;
using Subscriber.Domain;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;

namespace Subscriber.Outbound.Adapter;

public class AvroDeserializer : IDeserializer
{
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<TcpSubscriber>(LogSource.MessageBroker);
    
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
        catch (Exception ex)
        {
            Logger.LogError($"Error deserializing: {ex}");
            return Task.FromResult<object?>(null);
        }
    }
}