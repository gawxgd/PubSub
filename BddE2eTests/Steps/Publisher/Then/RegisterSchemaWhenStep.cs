using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using Reqnroll;

namespace BddE2eTests.Steps.Publisher.Then;

[Binding]
public class RegisterSchemaWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);
    
    [When(@"schema ""(.*)"" is registered for topic ""(.*)""")]
    [Given(@"schema ""(.*)"" is registered for topic ""(.*)""")]
    public async Task WhenSchemaKeyIsRegisteredForTopic(string schemaKey, string topic)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[Schema Step] Registering schema '{schemaKey}' for topic '{topic}'.. .");

        var schemaJson = TestSchemas.Resolve(schemaKey);
        await RegisterSchemaAsync(topic, schemaJson);

        await TestContext.Progress.WriteLineAsync(
            $"[Schema Step] Schema '{schemaKey}' registered for topic '{topic}'!");
    }

    [When(@"schema v1 is registered for topic ""(.*)""")]
    public async Task WhenSchemaV1IsRegisteredForTopic(string topic)
    {
        await WhenSchemaKeyIsRegisteredForTopic(TestSchemas.Keys.V1, topic);
    }
    
    [When(@"schema v2 is registered for topic ""(.*)""")]
    public async Task WhenSchemaV2IsRegisteredForTopic(string topic)
    {
        await WhenSchemaKeyIsRegisteredForTopic(TestSchemas.Keys.V2_PriorityNullable_DefaultNull, topic);
    }
    
    private async Task RegisterSchemaAsync(string topic, string schemaJson)
    {
        var options = _context.SchemaRegistryOptions;
        var adminClient = new SchemaRegistryAdminClient(
            options.Host, options.Port, TimeSpan.FromSeconds(options.TimeoutSeconds));
    
        await adminClient.RegisterSchemaAsync(topic, schemaJson);
    }
}