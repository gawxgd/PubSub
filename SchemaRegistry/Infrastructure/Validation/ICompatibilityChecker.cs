using Chr.Avro.Abstract;

namespace SchemaRegistry.Infrastructure.Validation
{
    public interface ICompatibilityChecker
    {
        bool IsBackwardCompatible(RecordSchema newSchemaJson, RecordSchema oldSchemaJson);
        
        bool IsForwardCompatible(RecordSchema newSchemaJson, RecordSchema oldSchemaJson);
    }
}