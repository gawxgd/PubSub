using System;
using System.Collections.Generic;

namespace BddE2eTests.Configuration.TestEvents;

public static class TestSchemas
{
    public static class Keys
    {
        public const string V1 = "v1";
        public const string V2_PriorityNullable_DefaultNull = "v2_priority_nullable_default_null";
        public const string V2_PriorityInt_Default0 = "v2_priority_int_default_0";
        public const string V2_PriorityInt_NoDefault = "v2_priority_int_no_default";
    }

    private static readonly IReadOnlyDictionary<string, string> SchemasByKey =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [Keys.V1] = ValidSchemaJson,
            [Keys.V2_PriorityNullable_DefaultNull] = ValidSchemaJsonV2,
            [Keys.V2_PriorityInt_Default0] = SchemaV2PriorityIntDefault0,
            [Keys.V2_PriorityInt_NoDefault] = SchemaV2PriorityIntNoDefault
        };

    public static string Resolve(string schemaKey)
    {
        if (string.IsNullOrWhiteSpace(schemaKey))
        {
            throw new ArgumentException("Schema key must be provided", nameof(schemaKey));
        }

        if (!SchemasByKey.TryGetValue(schemaKey, out var schemaJson))
        {
            throw new ArgumentException(
                $"Unknown schema key '{schemaKey}'. Known keys: {string.Join(", ", SchemasByKey.Keys)}",
                nameof(schemaKey));
        }

        return schemaJson;
    }

    /// <summary>
    /// Base schema v1 - used by default
    /// </summary>
    public const string ValidSchemaJson = @"
    {
        ""type"": ""record"",
          ""name"": ""TestEvent"",
          ""namespace"": ""BddE2eTests.TestEvents"",
          ""fields"": [
            { ""name"": ""Message"", ""type"": ""string"" }
            ]
    }";
    
    /// <summary>
    /// Schema v2 - (new optional field with default)
    /// </summary>
    public const string ValidSchemaJsonV2 = @"
    {
        ""type"":  ""record"",
        ""name"":  ""TestEvent"",
        ""namespace"": ""BddE2eTests.TestEvents"",
        ""fields"": [
            { ""name"": ""Message"", ""type"":  ""string"" },
            { ""name"": ""Priority"", ""type"": [""null"", ""int""], ""default"": null }
        ]
    }";

    /// <summary>
    /// Schema v2 - new required field with a default value (useful for backward-compat tests with deterministic default)
    /// </summary>
    public const string SchemaV2PriorityIntDefault0 = @"
    {
        ""type"":  ""record"",
        ""name"":  ""TestEvent"",
        ""namespace"": ""BddE2eTests.TestEvents"",
        ""fields"": [
            { ""name"": ""Message"", ""type"":  ""string"" },
            { ""name"": ""Priority"", ""type"": ""int"", ""default"": 0 }
        ]
    }";

    /// <summary>
    /// Schema v2 - new required field without default (forward-only safe change: old readers ignore unknown field)
    /// </summary>
    public const string SchemaV2PriorityIntNoDefault = @"
    {
        ""type"":  ""record"",
        ""name"":  ""TestEvent"",
        ""namespace"": ""BddE2eTests.TestEvents"",
        ""fields"": [
            { ""name"": ""Message"", ""type"":  ""string"" },
            { ""name"": ""Priority"", ""type"": ""int"" }
        ]
    }";
}