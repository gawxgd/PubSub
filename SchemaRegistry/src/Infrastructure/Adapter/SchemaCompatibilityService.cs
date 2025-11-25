using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using SchemaRegistry.Domain.Enums;
using SchemaRegistry.Domain.Port;
using SchemaRegistry.Infrastructure.Validation;

namespace SchemaRegistry.Infrastructure.Adapter;

public class SchemaCompatibilityService : ISchemaCompatibilityService
{
    private readonly ICompatibilityChecker _checker;

    public SchemaCompatibilityService(ICompatibilityChecker checker)
    {
        _checker = checker;
    }

    /// <summary>
    /// Interpret compatibility of two Avro schemas based on configuration in _compatMode
    /// </summary>
    public bool IsCompatible(string oldSchemaJson, string newSchemaJson, CompatibilityMode mode)
    {
        if (mode == CompatibilityMode.None) return true;

        var jsonReader = new JsonSchemaReader();
        var newSchema = jsonReader.Read(newSchemaJson);
        var oldSchema = jsonReader.Read(oldSchemaJson);

        if (newSchema is RecordSchema newRecord && oldSchema is RecordSchema oldRecord)
        {
            switch (mode)
            {
                case CompatibilityMode.Backward:
                    return _checker.IsBackwardCompatible(newRecord, oldRecord);
                case CompatibilityMode.Forward:
                    return _checker.IsForwardCompatible(newRecord, oldRecord);
                case CompatibilityMode.Full:
                    return _checker.IsBackwardCompatible(newRecord, oldRecord)
                           && _checker.IsForwardCompatible(newRecord, oldRecord);
                default:
                    return false;
            }
        }

        return false;
    }
}