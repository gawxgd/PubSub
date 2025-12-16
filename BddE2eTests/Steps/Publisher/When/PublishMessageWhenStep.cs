using NUnit.Framework;
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
        await TestContext.Progress.WriteLineAsync($"[When Step] Sending message '{message}' to topic '{topic}'...");
        await PublishSingleMessage(message, topic);
        await TestContext.Progress.WriteLineAsync($"[When Step] Message sent!");
        
        _context.SentMessage = message;
        _context.Topic = topic;
    }

    [When(@"the publisher sends messages in order:")]
    public async Task WhenThePublisherSendsMessagesInOrder(Table table)
    {
        await TestContext.Progress.WriteLineAsync($"[When Step] Sending {table.Rows.Count} messages in order...");
        foreach (var row in table.Rows)
        {
            var message = row["Message"];
            await TestContext.Progress.WriteLineAsync($"[When Step] Sending '{message}'...");
            await PublishSingleMessage(message, _context.Topic);
        }
        await TestContext.Progress.WriteLineAsync("[When Step] All messages sent!");
    }

    private async Task PublishSingleMessage(string message, string topic)
    {
        var publisher = _context.Publisher;
        var evt = new TestEvent
        {
            Message = message,
            Topic = topic
        };
        await publisher.PublishAsync(evt);
    }
}
