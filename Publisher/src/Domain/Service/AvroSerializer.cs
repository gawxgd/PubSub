using System.Buffers.Binary;
using Avro;
using Avro.IO;
using Avro.Reflect;
using Publisher.Domain.Port;
using Shared.Domain.Entities.SchemaRegistryClient;

namespace Publisher.Domain.Service;

public sealed class AvroSerializer<T> : IAvroSerializer<T>
{
    public byte[] Serialize(T message, SchemaInfo schemaInfo)
    {
        var schema = Schema.Parse(schemaInfo.Json);

        using var stream = new MemoryStream();

        Span<byte> schemaIdBytes = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(schemaIdBytes, schemaInfo.SchemaId);
        stream.Write(schemaIdBytes);

        var writer = new ReflectWriter<T>(schema);
        var encoder = new BinaryEncoder(stream);
        writer.Write(message, encoder);
        encoder.Flush();

        return stream.ToArray();
    }
}
