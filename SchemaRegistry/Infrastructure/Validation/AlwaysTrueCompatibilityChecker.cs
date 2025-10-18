namespace SchemaRegistry.Infrastructure.Validation;

// TODO: implement a real compatibility checker

public class AlwaysTrueCompatibilityChecker : ICompatibilityChecker
{
    public bool IsBackwardCompatible(string newWriterSchemaJson, string oldReaderSchemaJson)
    {
        return true;
    }

    public bool IsForwardCompatible(string newWriterSchemaJson, string oldReaderSchemaJson)
    {
        return true;
    }
}