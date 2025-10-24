using Chr.Avro.Representation;
using Chr.Avro.Abstract;

namespace SchemaRegistry.Infrastructure.Validation;

/// <summary>
/// An Avro scheme compatibility checker using the Chr.Avro library for easier manipulation on schema objects
/// </summary>
public class CompatibilityChecker : ICompatibilityChecker
{
    /// <remarks>
    /// Acceptable changes if a new schema is to be backward compatible:
    /// Deleting fields, adding fields with default values, changing field names but keeping the old name as an alias
    /// </remarks>
    public bool IsBackwardCompatible(RecordSchema newSchemaJson, RecordSchema oldSchemaJson)
    {
        throw new NotImplementedException();
    }

    /// <remarks>
    /// Acceptable changes if a new schema is to be forward compatible:
    /// Deleting fields that have a default value, adding fields, changing field names but keeping the old name as an alias
    /// </remarks>
    public bool IsForwardCompatible(RecordSchema newSchemaJson, RecordSchema oldSchemaJson)
    {
        throw new NotImplementedException();
    }
}