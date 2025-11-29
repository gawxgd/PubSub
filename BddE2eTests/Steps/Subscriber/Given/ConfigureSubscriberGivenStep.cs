using System.Threading.Channels;
using Reqnroll;
using Subscriber.Configuration;
using BddE2eTests.Configuration;

namespace BddE2eTests.Steps.Subscriber.Given;

[Binding]
public class ConfigureSubscriberGivenStep(ScenarioContext scenarioContext)
{
    private const string SettingColumn = "Setting";
    private const string ValueColumn = "Value";
    private const string TopicSetting = "topic";
    private const string BrokerSetting = "broker";
    private const string MinMessageLengthSetting = "min message length";
    private const string MaxMessageLengthSetting = "max message length";
    private const string PollIntervalSetting = "poll interval";
    private const string MaxRetryAttemptsSetting = "max retry attempts";
    private const char BrokerSeparator = ':';
    private const string TopicRequiredError = "Topic must be specified in the configuration table";

    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Given(@"a subscriber is configured with the following options:")]
    public async Task GivenASubscriberIsConfiguredWithTheFollowingOptions(Table table)
    {
        var builder = _context.GetOrCreateSubscriberOptionsBuilder();
        string? topic = null;

        foreach (var row in table.Rows)
        {
            var setting = row[SettingColumn];
            var value = row[ValueColumn];

            switch (setting.ToLowerInvariant())
            {
                case TopicSetting:
                    topic = value;
                    break;
                case BrokerSetting:
                    var brokerParts = value.Split(BrokerSeparator);
                    if (brokerParts.Length == 2)
                    {
                        builder.WithBrokerHost(brokerParts[0])
                               .WithBrokerPort(int.Parse(brokerParts[1]));
                    }
                    break;
                case MinMessageLengthSetting:
                    builder.WithMinMessageLength(int.Parse(value));
                    break;
                case MaxMessageLengthSetting:
                    builder.WithMaxMessageLength(int.Parse(value));
                    break;
                case PollIntervalSetting:
                    builder.WithPollInterval(TimeSpan.FromMilliseconds(int.Parse(value)));
                    break;
                case MaxRetryAttemptsSetting:
                    builder.WithMaxRetryAttempts(uint.Parse(value));
                    break;
            }
        }

        if (string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException(TopicRequiredError);
        }

        var receivedMessages = Channel.CreateUnbounded<string>();
        var connectionReadySignal = _context.ConnectionReadySignal;
        var messageReceivedSignal = _context.MessageReceivedSignal;
        
        var subscriberOptions = builder
            .WithTopic(topic)
            .Build();

        var subscriberFactory = new SubscriberFactory();
        var subscriber = subscriberFactory.CreateSubscriber(subscriberOptions, async (message) =>
        {
            await receivedMessages.Writer.WriteAsync(message);
            messageReceivedSignal.TrySetResult(message);
        });

        await subscriber.StartConnectionAsync();
        
        // Start processing messages in background
        _ = Task.Run(async () => await subscriber.StartMessageProcessingAsync());
        
        // Signal that connection is ready
        connectionReadySignal.SetResult();

        _context.Subscriber = subscriber;
        _context.ReceivedMessages = receivedMessages;
        _context.Topic = topic;
        
        // Wait for connection to be established
        await connectionReadySignal.Task;
    }
}

