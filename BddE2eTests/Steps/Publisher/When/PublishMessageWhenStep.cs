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
        await PublishSingleMessage(message);
        
        _context.SentMessage = message;
        _context.Topic = topic;
    }

    [When(@"the publisher sends messages in order:")]
    public async Task WhenThePublisherSendsMessagesInOrder(Table table)
    {
        foreach (var row in table.Rows)
        {
            var message = row["Message"];
            await PublishSingleMessage(message);
        }
    }

    private async Task PublishSingleMessage(string message)
    {
        var publisher = _context.Publisher;
        var messageBytes = Encoding.UTF8.GetBytes(message);
        await publisher.PublishAsync(messageBytes);
    }
}
