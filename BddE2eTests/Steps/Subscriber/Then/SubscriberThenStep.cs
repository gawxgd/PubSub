using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
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

        Assert.That(received.Message, Is.EqualTo(expectedMessage),
            $"Expected to receive '{expectedMessage}' but got '{received.Message}'");
    }

    [Then(@"the subscriber receives messages in order:")]
    public async Task ThenTheSubscriberReceivesMessagesInOrder(Table table)
    {
        TestContext.Progress.WriteLine($"[Then Step] Expecting {table.Rows.Count} messages in order...");
        var expectedMessages = table.Rows.Select(row => row["Message"]).ToList();

        for (var i = 0; i < expectedMessages.Count; i++)
        {
            TestContext.Progress.WriteLine($"[Then Step] Waiting for message {i + 1}...");
            var received = await ReadSingleMessage();
            TestContext.Progress.WriteLine($"[Then Step] Received: '{received.Message}'");
            Assert.That(received.Message, Is.EqualTo(expectedMessages[i]),
                $"Message at position {i}: expected '{expectedMessages[i]}' but got '{received.Message}'");
        }
    }

    private async Task<TestEvent> ReadSingleMessage()
    {
        var receivedMessages = _context.ReceivedMessages;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));

        try
        {
            return await receivedMessages.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            TestContext.Progress.WriteLine($"[Then Step] TIMEOUT after {TimeoutSeconds}s waiting for message!");
            Assert.Fail($"Timeout after {TimeoutSeconds}s waiting for message");
            throw;
        }
    }

    [AfterScenario]
    public async Task Cleanup()
    {
        TestContext.Progress.WriteLine("[Cleanup] Starting subscriber cleanup...");
        try
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

            TestContext.Progress.WriteLine("[Cleanup] Subscriber cleanup complete");
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"[Cleanup] Error during subscriber cleanup: {ex.Message}");
        }
    }
}