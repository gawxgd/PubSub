using System.Text;
using Reqnroll;

namespace BddE2eTests.Steps.Publisher.When;

[Binding]
public class PublishMessageWhenStep(ScenarioContext scenarioContext)
{
    private readonly TestContext _context = new(scenarioContext);

    [When(@"the publisher sends message ""(.*)"" to topic ""(.*)""")]
    public async Task WhenThePublisherSendsMessageToTopic(string message, string topic)
    {
        var publisher = _context.Publisher;
        
        var formattedMessage = $"{topic}:{message}\n";
        var messageBytes = Encoding.UTF8.GetBytes(formattedMessage);
        
        await publisher.PublishAsync(messageBytes);
        
        _context.SentMessage = message;
        _context.Topic = topic;
        
        // Give some time for message to be processed
        await Task.Delay(500);
    }
}

