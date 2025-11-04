using Chr.Avro.Representation;
using Chr.Avro.Abstract;

namespace SchemaRegistry.Infrastructure.Validation;

/// <summary>
/// An Avro scheme compatibility checker using the Chr.Avro library for easier manipulation on schema objects
/// TODO: handle type promotion int->long ...
/// TODO: handle name aliases - both forward and backward compatibility should allow changing names
/// TODO: consider accepting unions with types given in a different order 
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
                if (!SchemaEquals(field.Type, oldCounterpart.Type))
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
                if (!SchemaEquals(field.Type, newCounterpart.Type))
                    return false; // the field type has been changed - illegal TODO: is it?
                
                continue; // the field hasn't been touched
            }
            
            // the counterpart of the same name doesn't exist
            if (field.Default == null)
                return false; // an old field without a default value has been deleted and that's illegal
        }

        return true;
    }

    /// <summary>
    /// Check if two Avro schemas are the same.
    /// Can be used to check if two fields in an avro schema are the same type.
    /// </summary>
    /// <remarks>
    /// Keep in mind that the Type property of a RecordField object is in fact of type Schema.
    /// A field can be a whole schema itself. A simple field is represented as a PrimitiveSchema object.
    /// For example an int field is represented as IntSchema which inherits from PrimitiveSchema.
    /// </remarks>
    private bool SchemaEquals(Schema a, Schema b)
    {
        if (a.GetType() == b.GetType())
        {
            switch (a)
            {
                case ArraySchema arrA when b is ArraySchema arrB: // both are arrays
                    return SchemaEquals(arrA.Item, arrB.Item); //      but do they store the same type of items?

                case MapSchema mapA when b is MapSchema mapB: //   both are maps
                    return SchemaEquals(mapA.Value, mapB.Value);  // in avro map keys are strings but what about values?

                case UnionSchema unionA when b is UnionSchema unionB: // both are unions
                    if (unionA.Schemas.Count != unionB.Schemas.Count) // but do they unite the same types?
                        return false;
                    return unionA.Schemas.Zip(unionB.Schemas, SchemaEquals).All(x => x);

                case RecordSchema recA when b is RecordSchema recB:
                    return recA.FullName == recB.FullName;

                case EnumSchema enumA when b is EnumSchema enumB:
                    return enumA.FullName == enumB.FullName;

                case FixedSchema fixA when b is FixedSchema fixB: // fixed schemas are const size binary data
                    return fixA.FullName == fixB.FullName && fixA.Size == fixB.Size;

                case PrimitiveSchema:
                    return true; // they passed the type check with GetType already

                default:
                    return true;
            }
        }

        return false;
    }
}