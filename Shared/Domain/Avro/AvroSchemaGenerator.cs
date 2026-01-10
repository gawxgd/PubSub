using Chr.Avro.Abstract;
using Chr.Avro.Representation;

namespace Shared.Domain.Avro;

public static class AvroSchemaGenerator
{
    private static readonly SchemaBuilder Builder = new();
    private static readonly JsonSchemaWriter Writer = new();
    private static readonly object _lock = new();

    public static string GenerateSchemaJson<T>()
    {
        // SchemaBuilder from Chr.Avro is not thread-safe, especially when parsing nullable attributes
        // Multiple threads calling BuildSchema concurrently can cause IndexOutOfRangeException
        // in NullabilityInfoContext.GetNullableContext/GetNullabilityInfo
        lock (_lock)
        {
            var schema = Builder.BuildSchema<T>();
            return Writer.Write(schema);
        }
    }
}

