using BddE2eTests.Configuration;
using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.Publisher.Then;

[Binding]
public class PublishSuccessThenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"publish succeeds")]
    public Task ThenPublishSucceeds()
    {
        Assert.That(_context.PublishException, Is.Null,
            $"Expected publish to succeed, but it failed with: {_context.PublishException}");

        return Task.CompletedTask;
    }
}

