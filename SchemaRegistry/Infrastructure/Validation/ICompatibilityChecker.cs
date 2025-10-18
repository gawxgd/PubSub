namespace SchemaRegistry.Infrastructure.Validation
{
    public interface ICompatibilityChecker
    {
        bool IsBackwardCompatible(string newWriterSchemaJson, string oldReaderSchemaJson);
        
        bool IsForwardCompatible(string newWriterSchemaJson, string oldReaderSchemaJson);
    }
}