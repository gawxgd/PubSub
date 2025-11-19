using SchemaRegistry.Domain.Enums;

namespace SchemaRegistry.Domain.Port;

public interface ISchemaCompatibilityService
{
    bool IsCompatible(string oldSchemaJson, string newSchemaJson, CompatibilityMode mode);
}
