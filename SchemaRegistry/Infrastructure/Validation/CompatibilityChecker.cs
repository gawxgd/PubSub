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
    /// Deleting fields, adding fields with default values
    /// </remarks>
    public bool IsBackwardCompatible(RecordSchema newSchema, RecordSchema oldSchema)
    {
        var oldFields = oldSchema.Fields.ToDictionary(f => f.Name, f => f);
        foreach (var field in newSchema.Fields)
        {
            // check if a field of the same name exists in the oldSchema
            if (oldFields.TryGetValue(field.Name, out var oldCounterpart))
            {
                if (field.Type != oldCounterpart.Type)
                    return false; // the field type has been changed - illegal TODO: is it?
                
                continue; // the field hasn't been touched
            }
                
            // the counterpart of the same name doesn't exist
            if (field.Default == null)
                return false; // a new field without a default value has been added and that's illegal
        }

        return true;
    }

    /// <remarks>
    /// Acceptable changes if a new schema is to be forward compatible:
    /// Deleting fields that have a default value, adding fields, changing field names but keeping the old name as an alias
    /// </remarks>
    public bool IsForwardCompatible(RecordSchema newSchema, RecordSchema oldSchema)
    {
        var newFields = newSchema.Fields.ToDictionary(f => f.Name, f => f);
        foreach (var field in oldSchema.Fields)
        {
            // check if a field of the same name exists in the newSchema
            if (newFields.TryGetValue(field.Name, out var newCounterpart))
            {
                if (field.Type != newCounterpart.Type)
                    return false; // the field type has been changed - illegal TODO: is it?
                
                continue; // the field hasn't been touched
            }
            
            // the counterpart of the same name doesn't exist
            if (field.Default == null)
                return false; // an old field without a default value has been deleted and that's illegal
        }

        return true;
    }
}