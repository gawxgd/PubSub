using System.Buffers.Binary;
using Avro;
using Avro.IO;
using Avro.Reflect;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Publisher.Domain.Exceptions;
using Publisher.Domain.Port;
using Shared.Domain.Entities.SchemaRegistryClient;

namespace Publisher.Outbound.Adapter;

public sealed class AvroSerializer<T> : IAvroSerializer<T>
{
    private static readonly IAutoLogger Logger = AutoLoggerFactory.CreateLogger<AvroSerializer<T>>(LogSource.Publisher);

    public byte[] Serialize(T message, SchemaInfo schemaInfo)
    {
        try
        {
            var schema = Schema.Parse(schemaInfo.SchemaJson.GetRawText());

            using var stream = new MemoryStream();

            Span<byte> schemaIdBytes = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(schemaIdBytes, schemaInfo.SchemaId);
            stream.Write(schemaIdBytes);

            var writer = new ReflectWriter<T>(schema);
            var encoder = new BinaryEncoder(stream);
            writer.Write(message, encoder);
            encoder.Flush();

            //Logger.LogDebug($"Successfully serialized message with schema ID: {schemaInfo.SchemaId}");
            return stream.ToArray();
        }
        catch (Exception ex)
        {
            Logger.LogError($"Failed to serialize message with schema ID: {schemaInfo.SchemaId}", ex);
            throw new SerializationException($"Failed to serialize message with schema ID: {schemaInfo.SchemaId}", ex);
        }
    }
}