using Chr.Avro.Abstract;

namespace SchemaRegistry.Infrastructure.Validation
{
    public interface ICompatibilityChecker
    {
        bool IsBackwardCompatible(RecordSchema newSchema, RecordSchema oldSchema);
        
        bool IsForwardCompatible(RecordSchema newSchema, RecordSchema oldSchema);
    }
}
