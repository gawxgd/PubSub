using System.Threading.Channels;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;
using NUnit.Framework;
using Reqnroll;

namespace BddE2eTests.Steps.Subscriber.Then;

[Binding]
public class SubscriberConsumedMessagesThenStep(ScenarioContext scenarioContext)
{
    private const int TimeoutSeconds = 10;
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"subscriber C consumed messages up to offset (\d+)")]
    public async Task ThenSubscriberCConsumedMessagesUpToOffset(ulong expectedLastOffset)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[Then Step] Waiting for subscriber to consume messages up to offset {expectedLastOffset}...");

        var expectedMessageCount = (int)(expectedLastOffset + 1);
        var receivedCount = await WaitForMessagesAsync(_context.ReceivedMessages, expectedMessageCount);

        Assert.That(receivedCount, Is.EqualTo(expectedMessageCount),
            $"Expected {expectedMessageCount} messages but received {receivedCount}");

        _context.CommittedOffset = expectedLastOffset;
        
        await TestContext.Progress.WriteLineAsync(
            $"[Then Step] All {expectedMessageCount} messages consumed! Committed offset set to {expectedLastOffset} for restart.");
    }

    private async Task<int> WaitForMessagesAsync(Channel<TestEvent> receivedMessages, int expectedCount)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));
        var receivedCount = 0;

        try
        {
            await foreach (var message in receivedMessages.Reader.ReadAllAsync(cts.Token))
            {
                receivedCount++;
                await TestContext.Progress.WriteLineAsync(
                    $"[Then Step] Received message {receivedCount}/{expectedCount}: '{message.Message}'");

                if (receivedCount >= expectedCount)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            await TestContext.Progress.WriteLineAsync(
                $"[Then Step] TIMEOUT after {TimeoutSeconds}s waiting for messages!");
            Assert.Fail($"Timeout after {TimeoutSeconds}s waiting for {expectedCount} messages");
        }

        return receivedCount;
    }
}

