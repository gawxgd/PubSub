using NUnit.Framework;
using Reqnroll;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;

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
            _context.PublishException = null;
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

    [When(@"the publisher sends message ""(.*)"" priority (\d+) to topic ""(.*)""")]
    public async Task WhenThePublisherSendsMessageWithPriorityToTopic(string message, int priority, string topic)
    {
        try
        {
            _context.PublishException = null;
            await TestContext.Progress.WriteLineAsync(
                $"[When Step] Sending message '{message}' with priority {priority} to topic '{topic}'...");

            _context.TryGetPublisher(out var publisher);
            await PublishSingleMessage(publisher!, message, topic, priority);

            await TestContext.Progress.WriteLineAsync("[When Step] Message sent!");

            _context.SentMessage = message;
            _context.Topic = topic;
        }
        catch (Exception ex)
        {
            _context.PublishException = ex;
        }
    }

    [When(@"the publisher (.+) sends message ""(.*)"" to topic ""(.*)""")]
    public async Task WhenTheNamedPublisherSendsMessageToTopic(string publisherName, string message, string topic)
    {
        try
        {
            _context.PublishException = null;
            await TestContext.Progress.WriteLineAsync(
                $"[When Step] Publisher '{publisherName}' sending message '{message}' to topic '{topic}'...");

            var publisher = _context.GetPublisher(publisherName);
            await PublishSingleMessage(publisher, message, topic);

            await TestContext.Progress.WriteLineAsync("[When Step] Message sent!");
            _context.SentMessage = message;
            _context.Topic = topic;
        }
        catch (Exception ex)
        {
            _context.PublishException = ex;
        }
    }

    [When(@"the publisher (.+) sends message ""(.*)"" priority (\d+) to topic ""(.*)""")]
    public async Task WhenTheNamedPublisherSendsMessageWithPriorityToTopic(
        string publisherName,
        string message,
        int priority,
        string topic)
    {
        try
        {
            _context.PublishException = null;
            await TestContext.Progress.WriteLineAsync(
                $"[When Step] Publisher '{publisherName}' sending message '{message}' with priority {priority} to topic '{topic}'...");

            var publisher = _context.GetPublisher(publisherName);
            await PublishSingleMessage(publisher, message, topic, priority);

            await TestContext.Progress.WriteLineAsync("[When Step] Message sent!");
            _context.SentMessage = message;
            _context.Topic = topic;
        }
        catch (Exception ex)
        {
            _context.PublishException = ex;
        }
    }
    
    [When(@"the publisher sends a message")]
    public async Task WhenThePublisherSendsAMessage(Table table)
    {
        var message = table.Rows[0]["Message"];
        var topic = _context.Topic;

        int? priority = null;
        if (table.Rows.Count >= 2 && int.TryParse(table.Rows[1]["Message"], out var parsedPriority))
        {
            priority = parsedPriority;
        }

        _context.TryGetPublisher(out var publisher);
        await PublishSingleMessage(publisher!, message, topic, priority);

        _context.SentMessage = message;
    }
    
    [When(@"the publisher sends a message of type ""(.*)""")]
    public async Task WhenThePublisherSendsAMessageOfType(string eventType, Table table)
    {
        var message = table.Rows[0]["Message"];
        var topic = _context.Topic;

        int? priority = null;
        if (table.Rows.Count >= 2 && int.TryParse(table.Rows[1]["Message"], out var parsedPriority))
        {
            priority = parsedPriority;
        }
        
        _context.TryGetPublisher(out var publisher);
        var resolvedType = TestEventTypeResolver.Resolve(eventType);
        var evt = CreateEvent(resolvedType, message, topic, priority);
        await publisher!.PublishAsync(evt);

        _context.SentMessage = message;
    }

    [When(@"the publisher sends (\d+) messages to topic ""(.*)""")]
    [Given(@"(\d+) messages have been published to topic ""(.*)""")]
    public async Task WhenThePublisherSendsMessagesToTopic(int messageCount, string topic)
    {
        await TestContext.Progress.WriteLineAsync($"[When Step] Sending {messageCount} messages to topic '{topic}'...");
        
        _context.TryGetPublisher(out var publisher);
        
        for (var i = 0; i < messageCount; i++)
        {
            var message = $"msg{i}";
            await TestContext.Progress.WriteLineAsync($"[When Step] Sending message {i + 1}/{messageCount}: '{message}'...");

            await PublishSingleMessage(publisher!, message, topic);
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

    private async Task PublishMessagesInOrderAsync(PublisherHandle publisher, string publisherDescription, Table table)
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

    private async Task PublishSingleMessage(PublisherHandle publisher, string message, string topic)
    {
        await PublishSingleMessage(publisher, message, topic, priority: null);
    }

    private async Task PublishSingleMessage(PublisherHandle publisher, string message, string topic, int? priority)
    {
        var evt = CreateEvent(publisher.MessageType, message, topic, priority);
        await publisher.PublishAsync(evt);
    }
    
    private static ITestEvent CreateEvent(Type eventType, string message, string topic, int? priority)
    {
        if (!typeof(ITestEvent).IsAssignableFrom(eventType))
        {
            throw new ArgumentException($"Type '{eventType.Name}' does not implement {nameof(ITestEvent)}", nameof(eventType));
        }

        var evt = (ITestEvent)Activator.CreateInstance(eventType)!;
        evt.Message = message;
        evt.Topic = topic;

        if (priority.HasValue)
        {
            var priorityProp = eventType.GetProperty(nameof(TestEventWithAdditionalField.Priority));
            if (priorityProp == null)
            {
                throw new ArgumentException($"Type '{eventType.Name}' does not have a 'Priority' property");
            }
            priorityProp.SetValue(evt, priority.Value);
        }

        return evt;
    }
}
