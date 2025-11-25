using System.Threading.Channels;
using NUnit.Framework;
using Reqnroll;
using Subscriber.Configuration;
using Subscriber.Configuration.Options;
using Subscriber.Domain;

namespace BddE2eTests.Steps.Subscriber.Then;

[Binding]
public class SubscriberThenSteps(ScenarioContext scenarioContext)
{
    private readonly ScenarioContext _scenarioContext = scenarioContext;
    private ISubscriber? _subscriber;
    private Channel<string>? _receivedMessages = Channel.CreateUnbounded<string>();
    private const string BrokerHost = "127.0.0.1";
    private const int BrokerPort = 9096;

    [Then(@"a subscriber receives message ""(.*)"" from topic ""(.*)""")]
    public async Task ThenASubscriberReceivesMessageFromTopic(string expectedMessage, string topic)
    {
        // Set up subscriber before publisher sends message
        // This ensures subscriber is ready to receive
        var subscriberOptions = new SubscriberOptions
        {
            MessageBrokerConnectionUri = new Uri($"messageBroker://{BrokerHost}:{BrokerPort}"),
            Topic = topic,
            MinMessageLength = 1,
            MaxMessageLength = 1024,
            PollInterval = TimeSpan.FromMilliseconds(100),
            MaxRetryAttempts = 3
        };

        var subscriberFactory = new SubscriberFactory();
        _subscriber = subscriberFactory.CreateSubscriber(subscriberOptions, async (message) =>
        {
            await _receivedMessages!.Writer.WriteAsync(message);
        });

        await _subscriber.StartConnectionAsync();
        
        // Start processing messages in background
        _ = Task.Run(async () => await _subscriber.StartMessageProcessingAsync());
        
        // Give time for connection to establish and message to arrive
        await Task.Delay(1000);
        
        // Verify message was received
        var timeout = TimeSpan.FromSeconds(5);
        var cts = new CancellationTokenSource(timeout);
        
        try
        {
            var received = await _receivedMessages!.Reader.ReadAsync(cts.Token);
            Assert.That(received, Is.EqualTo(expectedMessage), 
                $"Expected to receive '{expectedMessage}' but got '{received}'");
        }
        catch (OperationCanceledException)
        {
            Assert.Fail($"Timeout waiting for message. Expected: '{expectedMessage}'");
        }
        finally
        {
            if (_subscriber is IAsyncDisposable subscriberDisposable)
            {
                await subscriberDisposable.DisposeAsync();
            }
            _receivedMessages?.Writer.Complete();
        }
    }

    [AfterScenario]
    public async Task Cleanup()
    {
        if (_subscriber is IAsyncDisposable subscriberDisposable)
        {
            await subscriberDisposable.DisposeAsync();
        }

        _receivedMessages?.Writer.Complete();
    }
}

