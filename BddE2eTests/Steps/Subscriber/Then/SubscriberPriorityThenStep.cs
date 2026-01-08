using System.Threading.Channels;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.Subscriber.Then;

[Binding]
public class SubscriberPriorityThenStep(ScenarioContext scenarioContext)
{
    private const int TimeoutSeconds = 10;

    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"the subscriber receives message ""(.*)"" with priority null")]
    public async Task ThenTheSubscriberReceivesMessageWithNullPriority(string message)
    {
        var received = await ReceiveSingleAsync(_context.ReceivedMessages);
        Assert.That(received.Message, Is.EqualTo(message));
        Assert.That(received.Priority, Is.Null);
    }

    [Then(@"the subscriber receives message ""(.*)"" with priority (\d+)")]
    public async Task ThenTheSubscriberReceivesMessageWithPriority(string message, int priority)
    {
        var received = await ReceiveSingleAsync(_context.ReceivedMessages);
        Assert.That(received.Message, Is.EqualTo(message));
        Assert.That(received.Priority, Is.EqualTo(priority));
    }

    private static async Task<TestEvent> ReceiveSingleAsync(Channel<TestEvent> receivedMessages)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));
        try
        {
            return await receivedMessages.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            Assert.Fail($"Timeout after {TimeoutSeconds}s waiting for a message");
            throw;
        }
    }
}

