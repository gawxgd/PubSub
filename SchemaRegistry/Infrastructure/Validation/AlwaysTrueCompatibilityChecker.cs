using Chr.Avro.Abstract;

namespace SchemaRegistry.Infrastructure.Validation;

public class AlwaysTrueCompatibilityChecker : ICompatibilityChecker
{
    public bool IsBackwardCompatible(RecordSchema newSchemaJson, RecordSchema oldSchemaJson)
    {
        return true;
    }

    public bool IsForwardCompatible(RecordSchema newSchemaJson, RecordSchema oldSchemaJson)
    {
        return true;
    }
}