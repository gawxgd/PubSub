using BddE2eTests.Configuration;
using NUnit.Framework;
using Reqnroll;
using Shared.Domain.Exceptions.SchemaRegistryClient;

namespace BddE2eTests.Steps.Publisher.Then;

[Binding]
public class SchemaIncompatibilityPublisherThenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"publish fails with schema incompatibility")]
    public Task ThenPublishFailsWithSchemaIncompatibility()
    {
        Assert.That(_context.PublishException, Is.Not.Null,
            "Expected publish to fail due to schema incompatibility, but no exception was captured");

        Assert.That(_context.PublishException, Is.TypeOf<SchemaIncompatibleException>(),
            $"Expected {nameof(SchemaIncompatibleException)}, but got {_context.PublishException!.GetType().Name}");

        return Task.CompletedTask;
    }
}

