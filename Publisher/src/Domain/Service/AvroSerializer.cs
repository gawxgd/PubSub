using System.Reflection;
using System.Text;
using Avro;
using Avro.Generic;
using Publisher.Domain.Model;
using Publisher.Domain.Port;

namespace Publisher.Domain.Service;

public sealed class AvroSerializer : IAvroSerializer
{
    private static readonly byte[] Separator = Encoding.UTF8.GetBytes("\n");
    
    public async Task<byte[]> SerializeAsync<T>(T message, SchemaInfo schemaInfo)
    {
        Schema schema;
        try
        {
            schema = Schema.Parse(schemaInfo.Json);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Failed to parse Avro schema", ex);
        }
        
        if (schema is not RecordSchema recordSchema)
            throw new InvalidOperationException("Top-level Avro schema must be a record.");
        
        // POCO object --> avro GenericRecord
        var record = new GenericRecord(recordSchema);
        FillRecord(record, message);

        // Serialize: filled avro GenericRecord --> byte[]
        byte[] serializedContent;
        using (var ms = new MemoryStream())
        {
            var encoder = new Avro.IO.BinaryEncoder(ms);
            var writer = new GenericWriter<GenericRecord>(recordSchema);

            try
            {
                writer.Write(record, encoder);
                encoder.Flush();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Avro serialization failed", ex);
            }

            serializedContent = ms.ToArray();
        }

        // Wrap serialized content with schema ID at the beginning
        byte[] wrappedSerializedMessage;
        using (var wrapperStream = new MemoryStream())
        using (var binaryWriter = new BinaryWriter(wrapperStream))
        {
            binaryWriter.Write(schemaInfo.SchemaId);
            binaryWriter.Write(serializedContent);
            wrappedSerializedMessage = wrapperStream.ToArray();
        }
        
        return wrappedSerializedMessage;
    }
    
    // EVERYTHING BELOW IS AI GENERATED TODO: read it
    
    // ---------------------------------------------------------
    // OBJECT → GenericRecord conversion
    // ---------------------------------------------------------

    private static void FillRecord(GenericRecord record, object obj)
    {
        var type = obj.GetType();
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        foreach (var field in record.Schema.Fields)
        {
            var prop = props.FirstOrDefault(p => 
                string.Equals(p.Name, field.Name, StringComparison.OrdinalIgnoreCase));

            if (prop == null)
            {
                // optional field → schema default
                record.Add(field.Name, field.DefaultValue);
                continue;
            }

            var value = prop.GetValue(obj);
            record.Add(field.Name, ConvertToAvroValue(value, field.Schema));
        }
    }

    private static object? ConvertToAvroValue(object? value, Schema schema)
    {
        if (schema is UnionSchema union)
            return ConvertUnion(value, union);

        if (value == null)
            return null;

        return schema switch
        {
            PrimitiveSchema primitive => ConvertPrimitive(value, primitive),
            RecordSchema recordSchema => ConvertRecord(value, recordSchema),
            ArraySchema arraySchema => ConvertArray(value, arraySchema),
            MapSchema mapSchema => ConvertMap(value, mapSchema),
            EnumSchema enumSchema => ConvertEnum(value, enumSchema),
            FixedSchema fixedSchema => ConvertFixed(value, fixedSchema),
            _ => throw new NotSupportedException($"Unsupported schema: {schema.Tag}")
        };
    }

    // ---------------------------------------------------------
    // Union handling
    // ---------------------------------------------------------

    private static object? ConvertUnion(object? value, UnionSchema union)
    {
        if (value == null)
        {
            var nullSchema = union.Schemas.FirstOrDefault(s => s.Tag == Schema.Type.Null);
            if (nullSchema != null) return null;
            throw new InvalidOperationException("Union does not accept null.");
        }

        foreach (var branch in union.Schemas)
        {
            try
            {
                return ConvertToAvroValue(value, branch);
            }
            catch
            {
                // continue trying
            }
        }

        throw new InvalidOperationException(
            $"Value '{value}' does not match any union branch.");
    }

    // ---------------------------------------------------------
    // Primitive types + logical types
    // ---------------------------------------------------------

    private static object ConvertPrimitive(object value, PrimitiveSchema schema)
    {
        switch (schema.Tag)
        {
            case Schema.Type.Boolean: return (bool)value;
            case Schema.Type.Int:
                // logical date?
                if (value is DateTime dt)
                    return (dt.Date - DateTime.UnixEpoch.Date).Days;
                return Convert.ToInt32(value);

            case Schema.Type.Long:
                // timestamp-millis?
                if (value is DateTime dt2)
                    return (long)(dt2 - DateTime.UnixEpoch).TotalMilliseconds;
                return Convert.ToInt64(value);

            case Schema.Type.Float: return Convert.ToSingle(value);
            case Schema.Type.Double: return Convert.ToDouble(value);
            case Schema.Type.String: return value.ToString();
            case Schema.Type.Bytes:
                if (value is byte[] bytes) return bytes;
                throw new InvalidOperationException("Expected byte[].");
            default:
                throw new NotSupportedException($"Unsupported primitive: {schema.Tag}");
        }
    }

    // ---------------------------------------------------------
    // Complex types
    // ---------------------------------------------------------

    private static object ConvertRecord(object value, RecordSchema schema)
    {
        var record = new GenericRecord(schema);
        FillRecord(record, value);
        return record;
    }

    private static object ConvertArray(object value, ArraySchema schema)
    {
        var list = new List<object?>();
        foreach (var item in (IEnumerable<object>)value)
            list.Add(ConvertToAvroValue(item, schema.ItemSchema));
        return list;
    }

    private static object ConvertMap(object value, MapSchema schema)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var kv in (IDictionary<string, object>)value)
            dict[kv.Key] = ConvertToAvroValue(kv.Value, schema.ValueSchema);
        return dict;
    }

    private static object ConvertEnum(object value, EnumSchema schema)
    {
        var str = value.ToString();
        if (!schema.Symbols.Contains(str))
            throw new InvalidOperationException($"Invalid enum value '{str}' for Avro enum.");
        return str;
    }

    private static object ConvertFixed(object value, FixedSchema schema)
    {
        var bytes = (byte[])value;
        if (bytes.Length != schema.Size)
            throw new InvalidOperationException("Fixed size mismatch.");
        return new GenericFixed(schema, bytes);
    }
}