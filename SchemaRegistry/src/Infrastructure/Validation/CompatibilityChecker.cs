using Chr.Avro.Representation;
using Chr.Avro.Abstract;

namespace SchemaRegistry.Infrastructure.Validation;

public class CompatibilityChecker : ICompatibilityChecker
{
    public bool IsBackwardCompatible(RecordSchema newSchema, RecordSchema oldSchema)
    {
        var oldFields = oldSchema.Fields.ToDictionary(f => f.Name, f => f);
        foreach (var field in newSchema.Fields)
        {
            if (oldFields.TryGetValue(field.Name, out var oldCounterpart))
            {
                if (!SchemaEquals(field.Type, oldCounterpart.Type))
                    return false;
                
                continue;
            }
                
            if (field.Default == null)
                return false;
        }

        return true;
    }

    public bool IsForwardCompatible(RecordSchema newSchema, RecordSchema oldSchema)
    {
        var newFields = newSchema.Fields.ToDictionary(f => f.Name, f => f);
        foreach (var field in oldSchema.Fields)
        {
            if (newFields.TryGetValue(field.Name, out var newCounterpart))
            {
                if (!SchemaEquals(field.Type, newCounterpart.Type))
                    return false;
                
                continue;
            }
            
            if (field.Default == null)
                return false;
        }

        return true;
    }

    private bool SchemaEquals(Schema a, Schema b)
    {
        if (a.GetType() == b.GetType())
        {
            switch (a)
            {
                case ArraySchema arrA when b is ArraySchema arrB:
                    return SchemaEquals(arrA.Item, arrB.Item);

                case MapSchema mapA when b is MapSchema mapB:
                    return SchemaEquals(mapA.Value, mapB.Value);

                case UnionSchema unionA when b is UnionSchema unionB:
                    if (unionA.Schemas.Count != unionB.Schemas.Count)
                        return false;
                    return unionA.Schemas.Zip(unionB.Schemas, SchemaEquals).All(x => x);

                case RecordSchema recA when b is RecordSchema recB:
                    return recA.FullName == recB.FullName;

                case EnumSchema enumA when b is EnumSchema enumB:
                    return enumA.FullName == enumB.FullName;

                case FixedSchema fixA when b is FixedSchema fixB:
                    return fixA.FullName == fixB.FullName && fixA.Size == fixB.Size;

                case PrimitiveSchema:
                    return true;

                default:
                    return true;
            }
        }

        return false;
    }
}
