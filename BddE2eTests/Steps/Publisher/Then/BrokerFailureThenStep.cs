using BddE2eTests.Configuration;
using MessageBroker.Domain.Enums;
using Reqnroll;
using Publisher.Outbound.Exceptions;

namespace BddE2eTests.Steps.Publisher.Then;

[Binding]
public class BrokerFailureThenStep(ScenarioContext scenarioContext)
{
    private const int TimeoutSeconds = 10;
    private readonly ScenarioTestContext _context = new(scenarioContext);
    
    [Then(@"the publisher reports that the topic is not available")]
    public async Task ThenPublisherReportsTopicNotAvailable()
    {
        _context.TryGetPublisher(out var publisher);
        
        var reader = publisher.ErrorResponses 
                     ?? throw new InvalidOperationException("ErrorResponses reader is not available");
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSeconds));
        
        try
        {
            await TestContext.Progress.WriteLineAsync(
                "[Then Step] Waiting for PublishResponse from broker...");
            
            var response = await reader.ReadAsync(cts.Token);
            
            await TestContext.Progress.WriteLineAsync(
                $"[Then Step] Received response: ErrorCode={response.ErrorCode}, BaseOffset={response.BaseOffset}");
            
            Assert.That(response. ErrorCode, Is.EqualTo(ErrorCode.TopicNotFound),
                $"Expected TopicNotFound error, but got {response.ErrorCode}");
        }
        catch (OperationCanceledException)
        {
            Assert.Fail($"Timeout after {TimeoutSeconds}s waiting for PublishResponse from broker");
        }
    }
}