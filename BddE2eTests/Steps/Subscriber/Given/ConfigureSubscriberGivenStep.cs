using System.Threading.Channels;
using Reqnroll;
using Subscriber.Configuration;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.Builder;
using BddE2eTests.Configuration.Options;
using BddE2eTests.Configuration.TestEvents;
using Subscriber.Outbound.Adapter;

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
    [When(@"a subscriber is configured with the following options:")]
    public async Task GivenASubscriberIsConfiguredWithTheFollowingOptions(Table table)
    {
        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Starting subscriber configuration...");
        var (subscriber, receivedMessages, topic) = await CreateSubscriberFromTableAsync<TestEvent>(table);

        _context.Subscriber = subscriber;
        _context.ReceivedMessages = receivedMessages;
        _context.Topic = topic;
    }
    
    [Given(@"a subscriber of type ""(.*)"" is configured with the following options:")]
    [When(@"a subscriber of type ""(.*)"" is configured with the following options:")]
    public async Task GivenASubscriberOfTypeIsConfiguredWithTheFollowingOptions(string eventType, Table table)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[Subscriber Step] Starting subscriber configuration for event type '{eventType}'...");
        
        var (subscriber, receivedMessages, topic) = await CreateSubscriberFromTableAsync(eventType, table);

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
            var (subscriber, receivedMessages, topic) = await CreateSubscriberFromTableAsync<TestEvent>(table);
            _context.SetSubscriber(name, subscriber, receivedMessages);
        }
        
        _context.Topic = ExtractTopicFromTable(table);
        await TestContext.Progress.WriteLineAsync($"[Subscriber Step] All {names.Length} subscribers configured!");
    }

    [Given(@"subscriber (.+) is configured starting at offset (\d+) with the following options:")]
    public async Task GivenSubscriberIsConfiguredStartingAtOffsetWithTheFollowingOptions(string subscriberName, ulong initialOffset, Table table)
    {
        await TestContext.Progress.WriteLineAsync($"[Subscriber Step] Starting subscriber '{subscriberName}' configuration at offset {initialOffset}...");
        var (subscriber, receivedMessages, topic) = await CreateSubscriberFromTableAsync<TestEvent>(table, initialOffset);

        _context.SetSubscriber(subscriberName, subscriber, receivedMessages);
        _context.Topic = topic;
        
        await TestContext.Progress.WriteLineAsync($"[Subscriber Step] Subscriber '{subscriberName}' configured at offset {initialOffset}!");
    }

    private Task<(SubscriberHandle subscriber, Channel<ITestEvent> receivedMessages, string topic)> CreateSubscriberFromTableAsync(
        string eventType,
        Table table,
        ulong? initialOffset = null)
    {
        var resolvedType = TestEventTypeResolver.Resolve(eventType);
        if (resolvedType == typeof(TestEvent))
        {
            return CreateSubscriberFromTableAsync<TestEvent>(table, initialOffset);
        }
        if (resolvedType == typeof(TestEventWithAdditionalField))
        {
            return CreateSubscriberFromTableAsync<TestEventWithAdditionalField>(table, initialOffset);
        }
        if (resolvedType == typeof(TestEventWithAdditionalDefaultField))
        {
            return CreateSubscriberFromTableAsync<TestEventWithAdditionalDefaultField>(table, initialOffset);
        }

        throw new ArgumentException($"Unsupported subscriber event type '{eventType}'", nameof(eventType));
    }
    
    private async Task<(SubscriberHandle subscriber, Channel<ITestEvent> receivedMessages, string topic)> CreateSubscriberFromTableAsync<TEvent>(
        Table table,
        ulong? initialOffset = null)
        where TEvent : class, ITestEvent, new()
    {
        var builder = CreateSubscriberOptionsBuilderFromTable(table);
        var topic = ExtractTopicFromTable(table);

        if (string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException(TopicRequiredError);
        }

        await TestContext.Progress.WriteLineAsync($"[Subscriber Step] Building options for topic: {topic}");
        var receivedMessages = Channel.CreateUnbounded<ITestEvent>();
        var connectionReady = new TaskCompletionSource();

        var subscriberOptions = builder
            .WithTopic(topic)
            .Build();

        var schemaRegistryBuilder = _context.GetOrCreateSchemaRegistryClientBuilder();
        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Creating schema registry client...");
        var schemaRegistryClient = schemaRegistryBuilder.Build();

        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Creating subscriber factory...");
        var subscriberFactory = new SubscriberFactory<TEvent>(schemaRegistryClient);

        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Creating subscriber...");
        var subscriber = subscriberFactory.CreateSubscriber(subscriberOptions,
            async (message) => { await receivedMessages.Writer.WriteAsync(message); });

        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Connecting to broker...");
        if (initialOffset.HasValue && subscriber is TcpSubscriber<TEvent> tcpSubscriber)
        {
            await tcpSubscriber.StartConnectionAsync(initialOffset.Value);
            await TestContext.Progress.WriteLineAsync($"[Subscriber Step] Subscriber connected at offset {initialOffset.Value}!");
        }
        else
        {
            await subscriber.StartConnectionAsync();
            await TestContext.Progress.WriteLineAsync("[Subscriber Step] Subscriber connected!");
        }

        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Starting message processing task...");
        _ = Task.Run(async () => await subscriber.StartMessageProcessingAsync());

        connectionReady.SetResult();

        await connectionReady.Task;
        await TestContext.Progress.WriteLineAsync("[Subscriber Step] Subscriber setup complete!");

        return (new SubscriberHandle(subscriber, typeof(TEvent), receivedMessages), receivedMessages, topic);
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
            if (_context.TryGetSubscriber(out var subscriber))
            {
                await subscriber.DisposeAsync();
            }

            if (_context.TryGetReceivedMessages(out var channel))
            {
                channel?.Writer.Complete();
            }

            foreach (var sub in _context.GetAllSubscribers())
            {
                await sub.DisposeAsync();
            }

            TestContext.Progress.WriteLine("[Cleanup] Subscriber cleanup complete");
        }
        catch (Exception ex)
        {
            TestContext.Progress.WriteLine($"[Cleanup] Error during subscriber cleanup: {ex.Message}");
        }
    }
}
