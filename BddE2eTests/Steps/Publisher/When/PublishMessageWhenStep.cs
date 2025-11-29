using System.Text;
using Reqnroll;
using BddE2eTests.Configuration;

namespace BddE2eTests.Steps.Publisher.When;

[Binding]
public class PublishMessageWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [When(@"the publisher sends message ""(.*)"" to topic ""(.*)""")]
    public async Task WhenThePublisherSendsMessageToTopic(string message, string topic)
    {
        var publisher = _context.Publisher;
        
        var messageBytes = Encoding.UTF8.GetBytes(message);
        
        await publisher.PublishAsync(messageBytes);
        
        _context.SentMessage = message;
        _context.Topic = topic;
    }
}

