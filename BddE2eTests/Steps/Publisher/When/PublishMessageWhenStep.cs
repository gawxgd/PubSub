using NUnit.Framework;
using Reqnroll;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using Publisher.Domain.Port;

namespace BddE2eTests.Steps.Publisher.When;

[Binding]
public class PublishMessageWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [When(@"the publisher sends message ""(.*)"" to topic ""(.*)""")]
    public async Task WhenThePublisherSendsMessageToTopic(string message, string topic)
    {
        try
        {
            await TestContext.Progress.WriteLineAsync($"[When Step] Sending message '{message}' to topic '{topic}'...");
            _context.TryGetPublisher(out var publisher);
            await PublishSingleMessage(publisher!, message, topic);
            await TestContext.Progress.WriteLineAsync($"[When Step] Message sent!");

            _context.SentMessage = message;
            _context.Topic = topic;
        }
        catch (Exception ex)
        {
            _context.PublishException = ex;
        }
    }

    [When(@"the publisher sends (\d+) messages to topic ""(.*)""")]
    public async Task WhenThePublisherSendsMessagesToTopic(int messageCount, string topic)
    {
        await TestContext.Progress.WriteLineAsync($"[When Step] Sending {messageCount} messages to topic '{topic}'...");
        
        for (var i = 0; i < messageCount; i++)
        {
            var message = $"msg{i}";
            await TestContext.Progress.WriteLineAsync($"[When Step] Sending message {i + 1}/{messageCount}: '{message}'...");
            
            var evt = new TestEvent
            {
                Message = message,
                Topic = topic
            };
            
            await _context.Publisher.PublishAsync(evt);
        }
        
        await TestContext.Progress.WriteLineAsync($"[When Step] All {messageCount} messages sent!");
    }

    [When(@"the publisher sends messages in order:")]
    public async Task WhenThePublisherSendsMessagesInOrder(Table table)
    {
        _context.TryGetPublisher(out var publisher);
        await PublishMessagesInOrderAsync(publisher!, "default publisher", table);
    }

    [When(@"the publisher (.+) sends messages in order:")]
    public async Task WhenThePublisherSendsMessagesInOrder(string publisherName, Table table)
    {
        var publisher = _context.GetPublisher(publisherName);
        await PublishMessagesInOrderAsync(publisher, $"publisher '{publisherName}'", table);
    }

    private async Task PublishMessagesInOrderAsync(IPublisher<TestEvent> publisher, string publisherDescription, Table table)
    {
        await TestContext.Progress.WriteLineAsync($"[When Step] Sending {table.Rows.Count} messages in order using {publisherDescription}...");
        foreach (var row in table.Rows)
        {
            var message = row["Message"];
            await TestContext.Progress.WriteLineAsync($"[When Step] Sending '{message}'...");
            await PublishSingleMessage(publisher, message, _context.Topic);
        }
        await TestContext.Progress.WriteLineAsync("[When Step] All messages sent!");
    }

    private async Task PublishSingleMessage(IPublisher<TestEvent> publisher, string message, string topic)
    {
        var evt = new TestEvent
        {
            Message = message,
            Topic = topic
        };
        await publisher.PublishAsync(evt);
    }
}
