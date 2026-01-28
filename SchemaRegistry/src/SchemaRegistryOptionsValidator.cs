using Microsoft.Extensions.Options;
using SchemaRegistry.Domain.Enums;

namespace SchemaRegistry;

public sealed class SchemaRegistryOptionsValidator(
    IConfiguration configuration,
    ILogger<SchemaRegistryOptionsValidator> logger)
    : IValidateOptions<SchemaRegistryOptions>
{
    private const string CompatibilityModeKey = "SchemaRegistry:CompatibilityMode";

    public ValidateOptionsResult Validate(string? name, SchemaRegistryOptions options)
    {
        var raw = configuration.GetValue<string>(CompatibilityModeKey);
        
        if (string.IsNullOrWhiteSpace(raw))
        {
            return ValidateOptionsResult.Success;
        }

        if (Enum.TryParse<CompatibilityMode>(raw, ignoreCase: true, out _))
        {
            return ValidateOptionsResult.Success;
        }

        logger.LogWarning(
            "Invalid value '{CompatibilityMode}' for {ConfigKey}. Allowed values: {Allowed}. Startup will fail.",
            raw,
            CompatibilityModeKey,
            string.Join(", ", Enum.GetNames<CompatibilityMode>()));

        return ValidateOptionsResult.Fail(
            $"Invalid '{CompatibilityModeKey}' value '{raw}'. Allowed values: {string.Join(", ", Enum.GetNames<CompatibilityMode>())}.");
    }
}

