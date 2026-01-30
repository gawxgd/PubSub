using NUnit.Framework;
using Reqnroll;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;

namespace BddE2eTests.Steps.CommitLog.When;

[Binding]
public class PublishWithPayloadWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);
    private static readonly TimeSpan AcknowledgmentTimeout = TimeSpan.FromSeconds(30);

    [When(@"the publisher sends (\d+) messages with (\d+)-byte payloads to topic ""(.*)""")]
    public async Task WhenThePublisherSendsMessagesWithPayloadsToTopic(int messageCount, int payloadSize, string topic)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog When] Sending {messageCount} messages with {payloadSize}-byte payloads to topic '{topic}'...");

        var basePadding = new string('X', Math.Max(0, payloadSize - 10));

        for (var i = 0; i < messageCount; i++)
        {
            var messagePrefix = $"msg{i}:";
            var paddingNeeded = Math.Max(0, payloadSize - messagePrefix.Length);
            var message = messagePrefix + basePadding.Substring(0, Math.Min(paddingNeeded, basePadding.Length));

            if (i % 10 == 0)
            {
                await TestContext.Progress.WriteLineAsync(
                    $"[CommitLog When] Sending message {i + 1}/{messageCount} (payload ~{message.Length} bytes)...");
            }

            var evt = new TestEvent
            {
                Message = message,
                Topic = topic
            };

            await _context.Publisher.PublishAsync(evt);
        }

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog When] Waiting for {messageCount} messages to be acknowledged...");

        var acknowledged = await _context.Publisher.WaitForAcknowledgmentsAsync(messageCount, AcknowledgmentTimeout);

        if (!acknowledged)
        {
            var currentAcks = _context.Publisher.AcknowledgedCount;
            throw new TimeoutException(
                $"Timed out waiting for acknowledgments. Expected {messageCount}, got {currentAcks} after {AcknowledgmentTimeout.TotalSeconds}s");
        }

        await TestContext.Progress.WriteLineAsync(
            $"[CommitLog When] All {messageCount} messages sent and acknowledged!");

        _context.Topic = topic;
    }
}

