using System.Threading.Channels;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.Subscriber.Then;

[Binding]
public class SubscriberOffsetPositionsThenStep(ScenarioContext scenarioContext)
{
    private const int TimeoutSeconds = 10;

    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"subscriber (.+) should receive only message ""(.*)""")]
    public async Task ThenSubscriberShouldReceiveOnlyMessage(string subscriberName, string expectedMessage)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[Then Step] Expecting subscriber '{subscriberName}' to receive only message '{expectedMessage}'...");

        var receivedMessages = _context.GetSubscriberReceivedMessages(subscriberName);
        var allReceivedMessages = new List<string>();
        var receivedExpectedMessage = false;
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));

        try
        {
            while (!receivedExpectedMessage)
            {
                var received = await receivedMessages.Reader.ReadAsync(cts.Token);
                allReceivedMessages.Add(received.Message);
                
                if (received.Message == expectedMessage)
                {
                    receivedExpectedMessage = true;
                    await TestContext.Progress.WriteLineAsync(
                        $"[Then Step] Subscriber '{subscriberName}' received expected message: '{received.Message}'");
                }
                else
                {
                    await TestContext.Progress.WriteLineAsync(
                        $"[Then Step] Subscriber '{subscriberName}' received unexpected message: '{received.Message}' (continuing to wait)");
                }
            }

            // Wait briefly to ensure no additional unique messages arrive
            using var additionalCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            try
            {
                while (true)
                {
                    var additionalMessage = await receivedMessages.Reader.ReadAsync(additionalCts.Token);
                    allReceivedMessages.Add(additionalMessage.Message);
                    
                    if (additionalMessage.Message != expectedMessage)
                    {
                        Assert.Fail(
                            $"Subscriber '{subscriberName}' received unexpected additional message: '{additionalMessage.Message}'. " +
                            $"Expected only '{expectedMessage}'. All received: [{string.Join(", ", allReceivedMessages)}]");
                    }
                    else
                    {
                        await TestContext.Progress.WriteLineAsync(
                            $"[Then Step] Subscriber '{subscriberName}' received duplicate of expected message (ignoring)");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected - no additional messages received
                await TestContext.Progress.WriteLineAsync(
                    $"[Then Step] Verified: subscriber '{subscriberName}' received only '{expectedMessage}' " +
                    $"(total messages including duplicates: {allReceivedMessages.Count})");
            }
        }
        catch (OperationCanceledException)
        {
            await TestContext.Progress.WriteLineAsync(
                $"[Then Step] TIMEOUT after {TimeoutSeconds}s waiting for message '{expectedMessage}'! " +
                $"Received: [{string.Join(", ", allReceivedMessages)}]");
            Assert.Fail(
                $"Timeout after {TimeoutSeconds}s waiting for subscriber '{subscriberName}' to receive '{expectedMessage}'. " +
                $"Received: [{string.Join(", ", allReceivedMessages)}]");
        }
    }

    [Then(@"subscriber (.+) should receive messages ""(.*)"" to ""(.*)"":")]
    public async Task ThenSubscriberShouldReceiveMessagesRange(string subscriberName, string fromMessage, string toMessage, Table table)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[Then Step] Expecting subscriber '{subscriberName}' to receive messages from '{fromMessage}' to '{toMessage}'...");

        var receivedMessages = _context.GetSubscriberReceivedMessages(subscriberName);
        var expectedMessages = table.Rows.Select(row => row["Message"]).ToList();
        var expectedSet = expectedMessages.ToHashSet();
        var receivedUniqueMessages = new List<string>();
        var allReceivedMessages = new List<string>();

        await TestContext.Progress.WriteLineAsync(
            $"[Then Step] Expected {expectedMessages.Count} messages: [{string.Join(", ", expectedMessages)}]");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));

        try
        {
            while (receivedUniqueMessages.Count < expectedMessages.Count)
            {
                var received = await receivedMessages.Reader.ReadAsync(cts.Token);
                allReceivedMessages.Add(received.Message);
                
                if (expectedSet.Contains(received.Message) && !receivedUniqueMessages.Contains(received.Message))
                {
                    receivedUniqueMessages.Add(received.Message);
                    await TestContext.Progress.WriteLineAsync(
                        $"[Then Step] Subscriber '{subscriberName}' received ({receivedUniqueMessages.Count}/{expectedMessages.Count}): '{received.Message}'");
                }
                else
                {
                    await TestContext.Progress.WriteLineAsync(
                        $"[Then Step] Subscriber '{subscriberName}' received duplicate/unexpected message: '{received.Message}' (skipping)");
                }
            }

            // Verify all expected messages were received in order
            for (var i = 0; i < expectedMessages.Count; i++)
            {
                Assert.That(receivedUniqueMessages[i], Is.EqualTo(expectedMessages[i]),
                    $"Subscriber '{subscriberName}' message at position {i}: expected '{expectedMessages[i]}' but got '{receivedUniqueMessages[i]}'. " +
                    $"Full received list: [{string.Join(", ", receivedUniqueMessages)}]");
            }

            await TestContext.Progress.WriteLineAsync(
                $"[Then Step] Subscriber '{subscriberName}' received all {expectedMessages.Count} expected messages in order! " +
                $"(Total messages including duplicates: {allReceivedMessages.Count})");
        }
        catch (OperationCanceledException)
        {
            await TestContext.Progress.WriteLineAsync(
                $"[Then Step] TIMEOUT after {TimeoutSeconds}s waiting for messages! " +
                $"Received so far: [{string.Join(", ", receivedUniqueMessages)}]");
            Assert.Fail(
                $"Timeout after {TimeoutSeconds}s waiting for subscriber '{subscriberName}' to receive messages. " +
                $"Expected: [{string.Join(", ", expectedMessages)}]. " +
                $"Received unique: [{string.Join(", ", receivedUniqueMessages)}]. " +
                $"All received: [{string.Join(", ", allReceivedMessages)}]");
        }
    }
}

