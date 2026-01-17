using System.Text.Json;
using BddE2eTests.Configuration;
using BddE2eTests.Steps.Publisher.Given;
using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.SchemaRegistry.Then;

[Binding]
public class SchemaRegistryPersistenceThenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"the schema registry still contains the same schema for topic ""(.*)""")]
    public async Task ThenSchemaRegistryStillContainsSameSchema(string topic)
    {
        var options = _context.SchemaRegistryOptions;
        var adminClient = new SchemaRegistryAdminClient(
            options.Host,
            options.Port,
            TimeSpan.FromSeconds(options.TimeoutSeconds));

        var expectedId = scenarioContext.Get<int>(RegisterTopicGivenStep.ExpectedSchemaIdKey);
        var expectedSchemaJson = scenarioContext.Get<string>(RegisterTopicGivenStep.ExpectedSchemaJsonKey);

        // Small retry to avoid flakiness right after restart.
        var loaded = await PollSchemaByIdAsync(adminClient, expectedId, timeout: TimeSpan.FromSeconds(5));

        Assert.That(loaded, Is.Not.Null, $"Expected schema id '{expectedId}' to exist after restart (topic '{topic}')");

        var normalizedActual = JsonSerializer.Serialize(loaded!.SchemaJson);
        Assert.That(normalizedActual, Is.EqualTo(expectedSchemaJson),
            "Expected schema JSON to be identical after restart (normalized JSON comparison)");
    }

    private static async Task<SchemaRegistryAdminClient.SchemaByIdResponse?> PollSchemaByIdAsync(
        SchemaRegistryAdminClient adminClient,
        int id,
        TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            var schema = await adminClient.GetSchemaByIdAsync(id);
            if (schema != null)
            {
                return schema;
            }

            await Task.Delay(100);
        }

        return null;
    }
}

