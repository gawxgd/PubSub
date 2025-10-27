using SchemaRegistry.Domain.Enums;

namespace SchemaRegistry.Domain.Services;

public interface ISchemaCompatibilityService
{
    bool IsCompatible(string oldSchemaJson, string newSchemaJson, CompatibilityMode mode);
}
