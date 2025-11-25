using System.Text;
using NUnit.Framework;
using Reqnroll;
using Publisher.Configuration;
using Publisher.Configuration.Options;
using Publisher.Domain.Port;

namespace BddE2eTests.Steps.Publisher.Given;

[Binding]
public class PublisherGivenSteps(ScenarioContext scenarioContext)
{
    private const string BrokerHost = "127.0.0.1";
    private const int BrokerPort = 9096;

    [Given(@"a publisher sends message ""(.*)"" to topic ""(.*)""")]
    public async Task GivenAPublisherSendsMessageToTopic(string message, string topic)
    {
        var publisherOptions = new PublisherOptions(
            MessageBrokerConnectionUri: new Uri($"messageBroker://{BrokerHost}:{BrokerPort}"),
            MaxPublisherQueueSize: 1000,
            MaxSendAttempts: 3,
            MaxRetryAttempts: 3
        );

        var publisherFactory = new PublisherFactory();
        var publisher = publisherFactory.CreatePublisher(publisherOptions);
        
        await publisher.CreateConnection();
        
        // Format message as "topic:message" based on subscriber expectations
        var formattedMessage = $"{topic}:{message}\n";
        var messageBytes = Encoding.UTF8.GetBytes(formattedMessage);
        
        await publisher.PublishAsync(messageBytes);
        
        scenarioContext["publisher"] = publisher;
        scenarioContext["sentMessage"] = message;
        scenarioContext["topic"] = topic;
        
        // Give some time for message to be processed
        await Task.Delay(500);
    }
}

