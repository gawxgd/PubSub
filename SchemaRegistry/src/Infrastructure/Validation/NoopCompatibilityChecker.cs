using Chr.Avro.Abstract;

namespace SchemaRegistry.Infrastructure.Validation;

public class NoopCompatibilityChecker : ICompatibilityChecker
{
    public bool IsBackwardCompatible(RecordSchema newSchema, RecordSchema oldSchema)
    {
        return true;
    }

    public bool IsForwardCompatible(RecordSchema newSchema, RecordSchema oldSchema)
    {
        return true;
    }
}