using Chr.Avro.Abstract;
using Chr.Avro.Representation;

namespace Shared.Domain.Avro;

public static class AvroSchemaGenerator
{
    private static readonly SchemaBuilder Builder = new();
    private static readonly JsonSchemaWriter Writer = new();

    public static string GenerateSchemaJson<T>()
    {
        var schema = Builder.BuildSchema<T>();
        return Writer.Write(schema);
    }
}

