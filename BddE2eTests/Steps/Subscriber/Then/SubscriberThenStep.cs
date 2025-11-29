using BddE2eTests.Configuration;
using Reqnroll;

namespace BddE2eTests.Steps.Subscriber.Then;

[Binding]
public class SubscriberThenStep(ScenarioContext scenarioContext)
{
    private const int TimeoutSeconds = 10;
    
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"a subscriber receives message ""(.*)"" from topic ""(.*)""")]
    public async Task ThenASubscriberReceivesMessageFromTopic(string expectedMessage, string topic)
    {
        var received = await ReadSingleMessage();
        
        Assert.That(received, Is.EqualTo(expectedMessage),
            $"Expected to receive '{expectedMessage}' but got '{received}'");
    }

    [Then(@"the subscriber receives messages in order:")]
    public async Task ThenTheSubscriberReceivesMessagesInOrder(Table table)
    {
        var expectedMessages = table.Rows.Select(row => row["Message"]).ToList();
        
        for (var i = 0; i < expectedMessages.Count; i++)
        {
            var received = await ReadSingleMessage();
            Assert.That(received, Is.EqualTo(expectedMessages[i]),
                $"Message at position {i}: expected '{expectedMessages[i]}' but got '{received}'");
        }
    }

    private async Task<string> ReadSingleMessage()
    {
        var receivedMessages = _context.ReceivedMessages;
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));
        
        try
        {
            return await receivedMessages.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            Assert.Fail($"Timeout after {TimeoutSeconds}s waiting for message");
            throw;
        }
    }

    [AfterScenario]
    public async Task Cleanup()
    {
        if (_context.TryGetSubscriber(out var subscriber)
            && subscriber is IAsyncDisposable subscriberDisposable)
        {
            await subscriberDisposable.DisposeAsync();
        }

        if (_context.TryGetReceivedMessages(out var channel))
        {
            channel?.Writer.Complete();
        }
    }
}
