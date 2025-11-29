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
        var receivedMessages = _context.ReceivedMessages;
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));
        
        try
        {
            var received = await receivedMessages.Reader.ReadAsync(cts.Token);
            Assert.That(received, Is.EqualTo(expectedMessage),
                $"Expected to receive '{expectedMessage}' but got '{received}'");
        }
        catch (OperationCanceledException)
        {
            Assert.Fail($"Timeout after {TimeoutSeconds}s waiting for message. Expected: '{expectedMessage}'");
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
