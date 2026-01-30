using Reqnroll;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.TestEvents;

namespace BddE2eTests.Steps.Publisher.When;

[Binding]
public class PublishDuringBrokerRestartWhenStep(ScenarioContext scenarioContext)
{
    private readonly ScenarioTestContext _context = new(scenarioContext);
    private static readonly TimeSpan AcknowledgmentTimeout = TimeSpan.FromSeconds(10);

    [When(@"the publisher sends (\d+) messages to topic ""(.*)"" and the broker restarts after (\d+) messages")]
    public async Task WhenThePublisherSendsMessagesAndBrokerRestarts(int totalMessageCount, string topic,
        int messagesBeforeRestart)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[When Step] Sending {totalMessageCount} messages to topic '{topic}', restarting broker after {messagesBeforeRestart} messages...");

        for (var i = 0; i < messagesBeforeRestart; i++)
        {
            var message = $"msg{i}";
            await TestContext.Progress.WriteLineAsync(
                $"[When Step] Sending message {i + 1}/{totalMessageCount}: '{message}'...");

            var evt = new TestEvent
            {
                Message = message,
                Topic = topic
            };

            await _context.Publisher.PublishAsync(evt);
        }

        await TestContext.Progress.WriteLineAsync(
            $"[When Step] Sent {messagesBeforeRestart} messages, waiting for broker acknowledgments...");

        var acknowledged = await _context.Publisher.WaitForAcknowledgmentsAsync(messagesBeforeRestart, AcknowledgmentTimeout);
        
        if (!acknowledged)
        {
            var currentAcks = _context.Publisher.AcknowledgedCount;
            throw new TimeoutException(
                $"Timed out waiting for broker acknowledgments. Expected {messagesBeforeRestart}, got {currentAcks} after {AcknowledgmentTimeout.TotalSeconds}s");
        }

        await TestContext.Progress.WriteLineAsync(
            $"[When Step] All {messagesBeforeRestart} messages acknowledged by broker, restarting broker...");

        await TestBase.RestartBrokerAsync();

        await TestContext.Progress.WriteLineAsync(
            $"[When Step] Broker restarted, continuing to send remaining messages...");

        for (var i = messagesBeforeRestart; i < totalMessageCount; i++)
        {
            var message = $"msg{i}";
            await TestContext.Progress.WriteLineAsync(
                $"[When Step] Sending message {i + 1}/{totalMessageCount}: '{message}'...");

            var evt = new TestEvent
            {
                Message = message,
                Topic = topic
            };

            await _context.Publisher.PublishAsync(evt);
        }

        var remainingMessages = totalMessageCount - messagesBeforeRestart;
        if (remainingMessages > 0)
        {
            await TestContext.Progress.WriteLineAsync(
                $"[When Step] Waiting for remaining {remainingMessages} messages to be acknowledged...");
            
            var remainingAcknowledged = await _context.Publisher.WaitForAcknowledgmentsAsync(totalMessageCount, AcknowledgmentTimeout);
            
            if (!remainingAcknowledged)
            {
                var currentAcks = _context.Publisher.AcknowledgedCount;
                throw new TimeoutException(
                    $"Timed out waiting for broker acknowledgments after restart. Expected {totalMessageCount}, got {currentAcks} after {AcknowledgmentTimeout.TotalSeconds}s");
            }
        }

        await TestContext.Progress.WriteLineAsync(
            $"[When Step] All {totalMessageCount} messages sent and acknowledged!");
    }
}
