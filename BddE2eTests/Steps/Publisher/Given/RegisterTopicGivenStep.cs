using BddE2eTests.Configuration;
using BddE2eTests.Configuration.Options;
using BddE2eTests.Configuration.TestEvents;
using Reqnroll;

namespace BddE2eTests.Steps.Publisher.Given;

[Binding]
public class RegisterTopicGivenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new (scenarioContext);
    
    [Given(@"the schema registry contains a schema for topic ""(.*)""")]
    public async Task GivenTheSchemaRegistryContainsASchemaForTopic(string topic)
    {
        var options = _context.SchemaRegistryOptions;

        var adminClient = new SchemaRegistryAdminClient(
            options.Host,
            options.Port,
            TimeSpan.FromSeconds(options.TimeoutSeconds));

        await adminClient.RegisterSchemaAsync(topic, TestSchemas.ValidSchemaJson);
    }
}