using BddE2eTests.Configuration;
using Reqnroll;

namespace BddE2eTests.Steps.Subscriber.Then;

[Binding]
public class SubscriberReceivesMultipleMessagesThenStep(ScenarioContext scenarioContext)
{
    private const int TimeoutSeconds = 10;
    private const string MessageColumn = "Message";
    
    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Then(@"the subscriber receives messages in order:")]
    public async Task ThenTheSubscriberReceivesMessagesInOrder(Table table)
    {
        var receivedMessages = _context.ReceivedMessages;
        var expectedMessages = table.Rows.Select(row => row[MessageColumn]).ToList();
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));
        
        for (var i = 0; i < expectedMessages.Count; i++)
        {
            try
            {
                var received = await receivedMessages.Reader.ReadAsync(cts.Token);
                Assert.That(received, Is.EqualTo(expectedMessages[i]),
                    $"Message at position {i}: expected '{expectedMessages[i]}' but got '{received}'");
            }
            catch (OperationCanceledException)
            {
                Assert.Fail($"Timeout waiting for message at position {i}. Expected: '{expectedMessages[i]}'");
            }
        }
    }
}

