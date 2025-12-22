using BddE2eTests.Configuration;
using Reqnroll;
using Shared.Domain.Exceptions.SchemaRegistryClient;

namespace BddE2eTests.Steps.Subscriber.Then;

[Binding]
public class PublisherThenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);
    
    [Then(@"publish operation fails at the serialization step")]
    public Task ThenPublishOperationFailsAtTheSerializationStep()
    {
        Assert.That(_context.PublishException, Is.Not.Null,
            "Expected the publish operation to fail, but no exception was thrown");
        
        var ex = _context.PublishException;
        Assert.That(ex, Is.TypeOf<SchemaNotFoundException>(),
            $"Expected a SchemaNotFoundException, but got {ex.GetType().Name}");
        
        return Task.CompletedTask;
    }

}