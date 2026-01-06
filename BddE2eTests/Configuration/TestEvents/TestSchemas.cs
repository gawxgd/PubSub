namespace BddE2eTests.Configuration.TestEvents;

public static class TestSchemas
{
    public const string ValidSchemaJson = @"
    {
        ""type"": ""record"",
          ""name"": ""TestEvent"",
          ""namespace"": ""BddE2eTests.TestEvents"",
          ""fields"": [
            { ""name"": ""Message"", ""type"": ""string"" }
            ]
    }";
}