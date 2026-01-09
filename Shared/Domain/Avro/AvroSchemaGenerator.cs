using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Shared.Domain.Avro;

public static class AvroSchemaGenerator
{
    private static readonly SchemaBuilder Builder = new();
    private static readonly JsonSchemaWriter Writer = new();

    public static string GenerateSchemaJson<T>()
    {
        var schema = Builder.BuildSchema<T>();
        var schemaJson = Writer.Write(schema);

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

