using Reqnroll;
using Publisher.Configuration;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.Builder;
using BddE2eTests.Configuration.Options;
using BddE2eTests.Configuration.TestEvents;

namespace BddE2eTests.Steps.Publisher.Given;

[Binding]
public class ConfigurePublisherGivenStep(ScenarioContext scenarioContext)
{
    private const string SettingColumn = "Setting";
    private const string ValueColumn = "Value";
    private const string TopicSetting = "topic";
    private const string BrokerSetting = "broker";
    private const string QueueSizeSetting = "queue size";
    private const string MaxRetryAttemptsSetting = "max retry attempts";
    private const string MaxSendAttemptsSetting = "max send attempts";
    private const char BrokerSeparator = ':';
    private const string TopicRequiredError = "Topic must be specified in the configuration table";
    
    private const int CleanupTimeoutSeconds = 5;

    private readonly ScenarioTestContext _context = new(scenarioContext);
    private static readonly TestOptions TestOptions = TestOptionsLoader.Load();

    [Given(@"a publisher is configured with the following options:")]
    [When(@"a publisher is configured with the following options:")]
    public async Task GivenAPublisherIsConfiguredWithTheFollowingOptions(Table table)
    {
        await TestContext.Progress.WriteLineAsync("[Publisher Step] Starting publisher configuration...");
        var publisher = await CreatePublisherFromTableAsync<TestEvent>(table);
        
        _context.Publisher = publisher;
        _context.Topic = ExtractTopicFromTable(table);
    }
    
    [Given(@"a publisher of type ""(.*)"" is configured with the following options:")]
    [When(@"a publisher of type ""(.*)"" is configured with the following options:")]
    public async Task GivenAPublisherOfTypeIsConfiguredWithTheFollowingOptions(string eventType, Table table)
    {
        await TestContext.Progress.WriteLineAsync(
            $"[Publisher Step] Starting publisher configuration for event type '{eventType}'...");
        
        var publisher = await CreatePublisherFromTableAsync(eventType, table);
        
        _context.Publisher = publisher;
        _context.Topic = ExtractTopicFromTable(table);
    }

    [When(@"publishers (.+) are configured with the following options:")]
    [Given(@"publishers (.+) are configured with the following options:")]
    public async Task GivenPublishersAreConfiguredWithTheFollowingOptions(string publisherNames, Table table)
    {
        var names = ParseNames(publisherNames);
        await TestContext.Progress.WriteLineAsync($"[Publisher Step] Starting configuration for {names.Length} publishers: {string.Join(", ", names)}...");
        
        foreach (var name in names)
        {
            await TestContext.Progress.WriteLineAsync($"[Publisher Step] Configuring publisher '{name}'...");
            var publisher = await CreatePublisherFromTableAsync<TestEvent>(table);
            _context.SetPublisher(name, publisher);
        }
        
        _context.Topic = ExtractTopicFromTable(table);
        await TestContext.Progress.WriteLineAsync($"[Publisher Step] All {names.Length} publishers configured!");
    }

    private Task<PublisherHandle> CreatePublisherFromTableAsync(string eventType, Table table)
    {
        var resolvedType = TestEventTypeResolver.Resolve(eventType);
        if (resolvedType == typeof(TestEvent))
        {
            return CreatePublisherFromTableAsync<TestEvent>(table);
        }
        if (resolvedType == typeof(TestEventWithAdditionalField))
        {
            return CreatePublisherFromTableAsync<TestEventWithAdditionalField>(table);
        }
        if (resolvedType == typeof(TestEventWithAdditionalDefaultField))
        {
            return CreatePublisherFromTableAsync<TestEventWithAdditionalDefaultField>(table);
        }

        throw new ArgumentException($"Unsupported publisher event type '{eventType}'", nameof(eventType));
    }

    private async Task<PublisherHandle> CreatePublisherFromTableAsync<TEvent>(Table table)
        where TEvent : class, ITestEvent
    {
        var builder = CreatePublisherOptionsBuilderFromTable(table);
        var topic = ExtractTopicFromTable(table);

        if (string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException(TopicRequiredError);
        }

        await TestContext.Progress.WriteLineAsync($"[Publisher Step] Building options for topic: {topic}");
        var publisherOptions = builder.Build();

        var schemaRegistryBuilder = _context.GetOrCreateSchemaRegistryClientBuilder();
        await TestContext.Progress.WriteLineAsync("[Publisher Step] Creating schema registry client factory...");
        var schemaRegistryClientFactory = schemaRegistryBuilder.BuildFactory();

        await TestContext.Progress.WriteLineAsync("[Publisher Step] Creating publisher factory...");
        var publisherFactory = new PublisherFactory<TEvent>(schemaRegistryClientFactory);

        await TestContext.Progress.WriteLineAsync("[Publisher Step] Creating publisher...");
        var publisher = publisherFactory.CreatePublisher(publisherOptions);

        await TestContext.Progress.WriteLineAsync("[Publisher Step] Connecting to broker...");
        await publisher.CreateConnection();
        await TestContext.Progress.WriteLineAsync("[Publisher Step] Publisher connected!");

        return new PublisherHandle(publisher, typeof(TEvent));
    }

    private PublisherOptionsBuilder CreatePublisherOptionsBuilderFromTable(Table table)
    {
        var builder = new PublisherOptionsBuilder(TestOptions.Publisher);

        foreach (var row in table.Rows)
        {
            var setting = row[SettingColumn];
            var value = row[ValueColumn];

            switch (setting.ToLowerInvariant())
            {
                case TopicSetting:
                    builder.WithTopic(value);
                    break;
                case BrokerSetting:
                    var brokerParts = value.Split(BrokerSeparator);
                    if (brokerParts.Length == 2)
                    {
                        builder.WithBrokerHost(brokerParts[0])
                            .WithBrokerPort(int.Parse(brokerParts[1]));
                    }
                    break;
                case QueueSizeSetting:
                    builder.WithMaxPublisherQueueSize(uint.Parse(value));
                    break;
                case MaxRetryAttemptsSetting:
                    builder.WithMaxRetryAttempts(uint.Parse(value));
                    break;
                case MaxSendAttemptsSetting:
                    builder.WithMaxSendAttempts(uint.Parse(value));
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
        await TestContext.Progress.WriteLineAsync("[Cleanup] Starting publisher cleanup...");

        if (_context.TryGetPublisher(out var publisher))
        {
            await DisposePublisherAsync(publisher, "default");
        }

        foreach (var pub in _context.GetAllPublishers())
        {
            await DisposePublisherAsync(pub, "named");
        }

        await TestContext.Progress.WriteLineAsync("[Cleanup] Publisher cleanup complete");
    }

    private async Task DisposePublisherAsync(PublisherHandle publisher, string type)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(CleanupTimeoutSeconds));
        try
        {
            var disposeTask = publisher.DisposeAsync().AsTask();
            var completedTask = await Task.WhenAny(disposeTask, Task.Delay(Timeout.Infinite, cts.Token));

            if (completedTask != disposeTask)
            {
                await TestContext.Progress.WriteLineAsync(
                    $"[Cleanup] WARNING: {type} Publisher dispose timed out after {CleanupTimeoutSeconds}s");
            }
        }
        catch (OperationCanceledException)
        {
            await TestContext.Progress.WriteLineAsync($"[Cleanup] {type} Publisher dispose was cancelled");
        }
        catch (Exception ex)
        {
            await TestContext.Progress.WriteLineAsync($"[Cleanup] Error disposing {type} publisher: {ex.Message}");
        }
    }
}
