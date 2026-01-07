namespace BddE2eTests.Configuration.TestEvents;

public static class TestSchemas
{
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
}