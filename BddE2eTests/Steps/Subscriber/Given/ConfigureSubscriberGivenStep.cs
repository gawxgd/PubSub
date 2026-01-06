using System.Threading.Channels;
using Reqnroll;
using Subscriber.Configuration;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.Builder;
using BddE2eTests.Configuration.Options;
using BddE2eTests.Configuration.TestEvents;
using Subscriber.Domain;

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
    private static readonly TestOptions TestOptions = TestOptionsLoader.Load();

    [Given(@"a subscriber is configured with the following options:")]
    public async Task GivenASubscriberIsConfiguredWithTheFollowingOptions(Table table)
    {
        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Starting subscriber configuration...");
        var (subscriber, receivedMessages, topic) = await CreateSubscriberFromTableAsync(table);

        _context.Subscriber = subscriber;
        _context.ReceivedMessages = receivedMessages;
        _context.Topic = topic;
    }

    [Given(@"subscribers (.+) are configured with the following options:")]
    public async Task GivenSubscribersAreConfiguredWithTheFollowingOptions(string subscriberNames, Table table)
    {
        var names = ParseNames(subscriberNames);
        await TestContext.Progress.WriteLineAsync($"[Subscriber Step] Starting configuration for {names.Length} subscribers: {string.Join(", ", names)}...");
        
        foreach (var name in names)
        {
            await TestContext.Progress.WriteLineAsync($"[Subscriber Step] Configuring subscriber '{name}'...");
            var (subscriber, receivedMessages, topic) = await CreateSubscriberFromTableAsync(table);
            _context.SetSubscriber(name, subscriber, receivedMessages);
        }
        
        _context.Topic = ExtractTopicFromTable(table);
        await TestContext.Progress.WriteLineAsync($"[Subscriber Step] All {names.Length} subscribers configured!");
    }

    private async Task<(ISubscriber<TestEvent> subscriber, Channel<TestEvent> receivedMessages, string topic)> CreateSubscriberFromTableAsync(Table table)
    {
        var builder = CreateSubscriberOptionsBuilderFromTable(table);
        var topic = ExtractTopicFromTable(table);

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

        var schemaRegistryBuilder = _context.GetOrCreateSchemaRegistryClientBuilder();
        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Creating schema registry client...");
        var schemaRegistryClient = schemaRegistryBuilder.Build();

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

        await connectionReady.Task;
        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Subscriber setup complete!");

        return (subscriber, receivedMessages, topic);
    }

    private SubscriberOptionsBuilder CreateSubscriberOptionsBuilderFromTable(Table table)
    {
        var builder = new SubscriberOptionsBuilder(TestOptions.Subscriber);

        foreach (var row in table.Rows)
        {
            var setting = row[SettingColumn];
            var value = row[ValueColumn];

            switch (setting.ToLowerInvariant())
            {
                case TopicSetting:
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

        return builder;
    }

    private string ExtractTopicFromTable(Table table)
    {
        foreach (var row in table.Rows)
        {
            var setting = row[SettingColumn];
            var value = row[ValueColumn];

            if (setting.ToLowerInvariant() == TopicSetting)
            {
                return value;
            }
        }

        return string.Empty;
    }

    private string[] ParseNames(string names)
    {
        return names.Split([',', ' '], StringSplitOptions.RemoveEmptyEntries)
            .Select(n => n.Trim())
            .ToArray();
    }

    [AfterScenario]
    public async Task Cleanup()
    {
        TestContext.Progress.WriteLine("[Cleanup] Starting subscriber cleanup...");
        try
        {
            if (_context.TryGetSubscriber(out var subscriber)
                && subscriber is IAsyncDisposable subscriberDisposable)
            {
                await subscriberDisposable.DisposeAsync();
            }

            if (_context.TryGetReceivedMessages(out var channel))
            {
                channel?.Writer.Complete();
            }

            foreach (var sub in _context.GetAllSubscribers())
            {
                if (sub is IAsyncDisposable disposable)
                {
                    await disposable.DisposeAsync();
                }
            }

            TestContext.Progress.WriteLine("[Cleanup] Subscriber cleanup complete");
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"[Cleanup] Error during subscriber cleanup: {ex.Message}");
        }
    }
}
