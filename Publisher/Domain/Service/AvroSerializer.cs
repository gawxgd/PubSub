using System.Text;
using System.Text.Json;
using Avro;
using Avro.Generic;
using Publisher.Domain.Model;
using Publisher.Domain.Port;

namespace Publisher.Domain.Service;

public sealed class AvroSerializer : IAvroSerializer
{
    public async Task<byte[]> SerializeAsync<T>(T message, SchemaInfo schemaInfo, string topic)
    {
        // convert the object to json
        string json;
        try
        {
            // convert the object to json
            json = JsonSerializer.Serialize(message);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException("Error during JSON serialization.", ex);
        }
        
        JsonDocument jsonDoc;
        try
        {
            using var doc = JsonDocument.Parse(json);
            jsonDoc = doc;
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException("Error parsing JSON.", ex);
        }
        
        // parse avro schemaInfo: json -> Apache.Avro.SchemaInfo
        Schema avroSchema;
        try
        {
            // parse avro schemaInfo: json -> Apache.Avro.SchemaInfo
            avroSchema = Avro.Schema.Parse(schemaInfo.Json);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Error parsing Avro scheme.", ex);
        }

        if (avroSchema is not RecordSchema recordSchema)
            throw new InvalidOperationException("Avro schemaInfo must be a record at the top level.");

        // convert the json to GenericRecord
        var genericRecord = new GenericRecord(recordSchema);
        FillGenericRecord(genericRecord, jsonDoc.RootElement);
        
        // use avro library methods to serialize the generic record according to the avro schemaInfo 
        byte[] avroBytes;
        try
        {
            using (var ms = new MemoryStream())
            {
                var encoder = new Avro.IO.BinaryEncoder(ms);
                var writer = new GenericWriter<GenericRecord>(recordSchema);
                writer.Write(genericRecord, encoder);
                encoder.Flush();

                avroBytes = ms.ToArray();
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Error serializing a record.", ex);
        }

        // wrap the message like: topic:message\n
        var topicBytes = Encoding.UTF8.GetBytes(topic);
        var separator = Encoding.UTF8.GetBytes(":");
        var newline = Encoding.UTF8.GetBytes("\n");

        var finalBytes = new byte[
            topicBytes.Length +
            separator.Length +
            avroBytes.Length +
            newline.Length
        ];

        int offset = 0;

        Buffer.BlockCopy(topicBytes, 0, finalBytes, offset, topicBytes.Length);
        offset += topicBytes.Length;

        Buffer.BlockCopy(separator, 0, finalBytes, offset, separator.Length);
        offset += separator.Length;

        Buffer.BlockCopy(avroBytes, 0, finalBytes, offset, avroBytes.Length);
        offset += avroBytes.Length;

        Buffer.BlockCopy(newline, 0, finalBytes, offset, newline.Length);

        return finalBytes;
    }
    
    // -------------------------------------------------------------------------
    // Recursively fill GenericRecord according to Avro schemaInfo fields
    // -------------------------------------------------------------------------
    private static void FillGenericRecord(GenericRecord record, JsonElement json)
    {
        foreach (var field in record.Schema.Fields)
        {
            var fieldName = field.Name;

            if (!json.TryGetProperty(fieldName, out var jsonField))
            {
                // optional field → use default
                record.Add(fieldName, field.DefaultValue);
                continue;
            }

            record.Add(fieldName, ConvertJsonToAvro(jsonField, field.Schema));
        }
    }

    private static object? ConvertJsonToAvro(JsonElement json, Avro.Schema schema)
    {
        switch (schema)
        {
            case PrimitiveSchema primitive:
                return ConvertPrimitive(json, primitive);

            case RecordSchema recordSchema:
                var nested = new GenericRecord(recordSchema);
                FillGenericRecord(nested, json);
                return nested;

            case ArraySchema arraySchema:
                var list = new List<object?>();
                foreach (var el in json.EnumerateArray())
                    list.Add(ConvertJsonToAvro(el, arraySchema.ItemSchema));
                return list;

            case MapSchema mapSchema:
                var dict = new Dictionary<string, object?>();
                foreach (var prop in json.EnumerateObject())
                    dict[prop.Name] = ConvertJsonToAvro(prop.Value, mapSchema.ValueSchema);
                return dict;

            case UnionSchema unionSchema:
                // pick first matching branch
                foreach (var branch in unionSchema.Schemas)
                {
                    try { return ConvertJsonToAvro(json, branch); }
                    catch { /* ignore */ }
                }
                throw new InvalidOperationException("No matching union type found.");

            default:
                throw new NotSupportedException($"Unsupported Avro schemaInfo type: {schema.Tag}");
        }
    }

    private static object? ConvertPrimitive(JsonElement json, PrimitiveSchema schema)
    {
        return schema.Tag switch
        {
            Avro.Schema.Type.Boolean => json.GetBoolean(),
            Avro.Schema.Type.Int => json.GetInt32(),
            Avro.Schema.Type.Long => json.GetInt64(),
            Avro.Schema.Type.Float => json.GetSingle(),
            Avro.Schema.Type.Double => json.GetDouble(),
            Avro.Schema.Type.String => json.GetString(),
            Avro.Schema.Type.Bytes => json.GetBytesFromBase64(),
            _ => throw new NotSupportedException($"Unsupported primitive type: {schema.Tag}")
        };
    }
}



    

    
// wrap the serialized message like this: topic-message-separator
    
// return byte[]