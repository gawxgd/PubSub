using System.Linq;
using System.Threading.Channels;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.Subscriber.Then;

[Binding]
public class SubscriberReceivesMessagesThenStep(ScenarioContext scenarioContext)
{
    private const int TimeoutSeconds = 10;

    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"the subscriber receives messages:")]
    public async Task ThenTheSubscriberReceivesMessages(Table table)
    {
        var receivedMessages = _context.ReceivedMessages;
        await ReceiveMessagesAsync(receivedMessages, "default subscriber", table);
    }

    [Then(@"the subscriber (.+) receives messages:")]
    public async Task ThenTheSubscriberReceivesMessages(string subscriberName, Table table)
    {
        var receivedMessages = _context.GetSubscriberReceivedMessages(subscriberName);
        await ReceiveMessagesAsync(receivedMessages, $"subscriber '{subscriberName}'", table);
    }

    [Then(@"the subscriber successfully receives (\d+) messages")]
    public async Task ThenTheSubscriberSuccessfullyReceivesMessages(int expectedCount)
    {
        var receivedMessages = _context.ReceivedMessages;
        await ReceiveMessageCountAsync(receivedMessages, "default subscriber", expectedCount);
    }

    [Then(@"the subscriber ""(.*)"" successfully receives (\d+) messages")]
    public async Task ThenNamedSubscriberSuccessfullyReceivesMessages(string subscriberName, int expectedCount)
    {
        var receivedMessages = _context.GetSubscriberReceivedMessages(subscriberName);
        await ReceiveMessageCountAsync(receivedMessages, $"subscriber '{subscriberName}'", expectedCount);
    }

    private async Task ReceiveMessageCountAsync(Channel<ITestEvent> receivedMessages, string subscriberDescription, int expectedCount)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[Then Step] Expecting {expectedCount} messages for {subscriberDescription}.. .");
    
        var allReceivedMessages = new List<ITestEvent>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));

        try
        {
            while (allReceivedMessages.Count < expectedCount)
            {
                var received = await receivedMessages.Reader.ReadAsync(cts.Token);
                allReceivedMessages.Add(received);
                await TestContext.Progress.WriteLineAsync(
                    $"[Then Step] Received:  '{received.Message}' ({allReceivedMessages.Count}/{expectedCount})");
            }
        }
        catch (OperationCanceledException)
        {
            Assert.Fail(
                $"Timeout after {TimeoutSeconds}s.  Expected {expectedCount} messages, received {allReceivedMessages.Count}");
        }

        Assert.That(allReceivedMessages, Has.Count.EqualTo(expectedCount),
            $"Expected {expectedCount} messages but received {allReceivedMessages.Count}");
        
        await TestContext.Progress. WriteLineAsync(
            $"[Then Step] Successfully received all {expectedCount} messages!");
    }

    private async Task ReceiveMessagesAsync(Channel<ITestEvent> receivedMessages, string subscriberDescription, Table table)
    {
        TestContext.Progress.WriteLine($"[Then Step] Expecting {table.Rows.Count} messages for {subscriberDescription} (order not required)...");
        var expectedMessages = table.Rows.Select(row => row["Message"]).ToHashSet();
        var receivedMessageSet = new HashSet<string>();
        var allReceivedMessages = new List<string>();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));

        try
        {
            while (receivedMessageSet.Count < expectedMessages.Count)
            {
                var received = await receivedMessages.Reader.ReadAsync(cts.Token);
                var message = received.Message;
                allReceivedMessages.Add(message);
                receivedMessageSet.Add(message);
                TestContext.Progress.WriteLine($"[Then Step] Received from {subscriberDescription}: '{message}' ({receivedMessageSet.Count}/{expectedMessages.Count})");
            }
        }
        catch (OperationCanceledException)
        {
            TestContext.Progress.WriteLine($"[Then Step] TIMEOUT after {TimeoutSeconds}s waiting for messages!");
            Assert.Fail($"Timeout after {TimeoutSeconds}s waiting for {expectedMessages.Count} messages. Received: {string.Join(", ", receivedMessageSet)}");
        }

        var missingMessages = expectedMessages.Except(receivedMessageSet).ToList();
        if (missingMessages.Any())
        {
            Assert.Fail($"Subscriber {subscriberDescription} did not receive all expected messages. Missing: {string.Join(", ", missingMessages)}. Received: {string.Join(", ", allReceivedMessages)}");
        }

        TestContext.Progress.WriteLine($"[Then Step] Subscriber {subscriberDescription} received all {expectedMessages.Count} expected messages: {string.Join(", ", allReceivedMessages)}");
    }
}

