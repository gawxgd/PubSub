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
    private const string PollIntervalSetting = "poll interval";
    private const string MaxRetryAttemptsSetting = "max retry attempts";
    private const char BrokerSeparator = ':';
    private const string TopicRequiredError = "Topic must be specified in the configuration table";

    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Given(@"a subscriber is configured with the following options:")]
    public async Task GivenASubscriberIsConfiguredWithTheFollowingOptions(Table table)
    {
        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Starting subscriber configuration...");
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

        await TestContext.Progress.WriteLineAsync($"[Subscriber Step] Building options for topic: {topic}");
        var receivedMessages = Channel.CreateUnbounded<TestEvent>();
        var connectionReady = new TaskCompletionSource();

        var subscriberOptions = builder
            .WithTopic(topic)
            .Build();

        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Creating schema registry client...");
        var schemaRegistryClient =
            TestDependencies.CreateSchemaRegistryClient(subscriberOptions.SchemaRegistryConnectionUri);

        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Creating subscriber factory...");
        var subscriberFactory = new SubscriberFactory<TestEvent>(schemaRegistryClient);

        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Creating subscriber...");
        var subscriber = subscriberFactory.CreateSubscriber(subscriberOptions,
            async (message) => { await receivedMessages.Writer.WriteAsync(message); });

        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Connecting to broker...");
        await subscriber.StartConnectionAsync();
        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Subscriber connected!");

        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Starting message processing task...");
        _ = Task.Run(async () => await subscriber.StartMessageProcessingAsync());

        connectionReady.SetResult();

        _context.Subscriber = subscriber;
        _context.ReceivedMessages = receivedMessages;
        _context.Topic = topic;

        await connectionReady.Task;
        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Subscriber setup complete!");
    }
    
    //ToDo add cleanup
}