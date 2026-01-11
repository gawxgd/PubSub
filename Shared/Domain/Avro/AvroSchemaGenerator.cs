using Chr.Avro.Abstract;
using Chr.Avro.Representation;

namespace Shared.Domain.Avro;

public class AvroSchemaGenerator
{
    private readonly SchemaBuilder _builder = new();
    private readonly JsonSchemaWriter _writer = new();

    public string GenerateSchemaJson<T>()
    {
        var schema = _builder.BuildSchema<T>();
        return _writer.Write(schema);
    }
}

