using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using Reqnroll;
using Shared.Domain.Avro;
using System.Text.Json;

namespace BddE2eTests.Steps.Publisher.Given;

[Binding]
public class RegisterTopicGivenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new (scenarioContext);

    internal const string ExpectedSchemaIdKey = "ExpectedSchemaId";
    internal const string ExpectedSchemaJsonKey = "ExpectedSchemaJson";
    
    [Given(@"the schema registry contains a schema for topic ""(.*)""")]
    public async Task GivenTheSchemaRegistryContainsASchemaForTopic(string topic)
    {
        var options = _context.SchemaRegistryOptions;

        var adminClient = new SchemaRegistryAdminClient(
            options.Host,
            options.Port,
            TimeSpan.FromSeconds(options.TimeoutSeconds));

        var schemaJson = AvroSchemaGenerator.GenerateSchemaJson<TestEvent>();
        var schemaId = await adminClient.RegisterSchemaAsync(topic, schemaJson);
        scenarioContext.Set(schemaId, ExpectedSchemaIdKey);

        // Normalize JSON so comparisons are whitespace/format independent.
        var normalizedExpected = JsonSerializer.Serialize(JsonDocument.Parse(schemaJson).RootElement);
        scenarioContext.Set(normalizedExpected, ExpectedSchemaJsonKey);
    }
}