using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using Reqnroll;

namespace BddE2eTests.Steps.Publisher.Then;

[Binding]
public class RegisterSchemaWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);
    
    [When(@"schema v1 is registered for topic ""(.*)""")]
    public async Task WhenSchemaV1IsRegisteredForTopic(string topic)
    {
        await TestContext.Progress.WriteLineAsync($"[Schema Step] Registering schema v1 for topic '{topic}'.. .");
        
        await RegisterSchemaAsync(topic, TestSchemas.ValidSchemaJson);
        
        await TestContext.Progress.WriteLineAsync($"[Schema Step] Schema v1 registered for topic '{topic}'!");
    }
    
    [When(@"schema v2 is registered for topic ""(.*)""")]
    public async Task WhenSchemaV2IsRegisteredForTopic(string topic)
    {
        await TestContext.Progress.WriteLineAsync($"[Schema Step] Registering schema v2 for topic '{topic}'.. .");
        
        await RegisterSchemaAsync(topic, TestSchemas.ValidSchemaJsonV2);
        
        await TestContext.Progress. WriteLineAsync($"[Schema Step] Schema v2 registered for topic '{topic}'!");
    }
    
    private async Task RegisterSchemaAsync(string topic, string schemaJson)
    {
        var options = _context.SchemaRegistryOptions;
        var adminClient = new SchemaRegistryAdminClient(
            options.Host, options.Port, TimeSpan.FromSeconds(options.TimeoutSeconds));
    
        await adminClient.RegisterSchemaAsync(topic, schemaJson);
    }
}