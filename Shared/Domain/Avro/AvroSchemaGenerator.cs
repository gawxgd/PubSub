using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Shared.Domain.Avro;

public class AvroSchemaGenerator
{
    private readonly SchemaBuilder _builder = new();
    private readonly JsonSchemaWriter _writer = new();

    public string GenerateSchemaJson<T>()
    {
        var schema = _builder.BuildSchema<T>();
        var schemaJson = _writer.Write(schema);

        var recordName = typeof(T).GetCustomAttribute<AvroRecordNameAttribute>();
        if (recordName == null)
        {
            return schemaJson;
        }

        var root = JsonNode.Parse(schemaJson)?.AsObject();
        if (root == null)
        {
            return schemaJson;
        }

        root["name"] = recordName.Name;
        if (!string.IsNullOrWhiteSpace(recordName.Namespace))
        {
            root["namespace"] = recordName.Namespace;
        }

        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = false });
    }
}

