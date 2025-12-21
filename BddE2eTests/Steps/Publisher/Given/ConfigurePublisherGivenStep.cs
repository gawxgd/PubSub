using Reqnroll;
using Publisher.Configuration;
using BddE2eTests.Configuration;
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

    [Given(@"a publisher is configured with the following options:")]
    public async Task GivenAPublisherIsConfiguredWithTheFollowingOptions(Table table)
    {
        await TestContext.Progress.WriteLineAsync("[Publisher Step] Starting publisher configuration...");
        var builder = _context.GetOrCreatePublisherOptionsBuilder();
        var schemaRegistryBuilder = _context.GetOrCreateSchemaRegistryClientBuilder();
        string? topic = null;

        foreach (var row in table.Rows)
        {
            var setting = row[SettingColumn];
            var value = row[ValueColumn];

            switch (setting.ToLowerInvariant())
            {
                case TopicSetting:
                    topic = value;
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

        if (string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException(TopicRequiredError);
        }

        await TestContext.Progress.WriteLineAsync($"[Publisher Step] Building options for topic: {topic}");
        var publisherOptions = builder.Build();

        await TestContext.Progress.WriteLineAsync("[Publisher Step] Creating schema registry client factory...");
        var schemaRegistryClientFactory = schemaRegistryBuilder.BuildFactory();

        await TestContext.Progress.WriteLineAsync("[Publisher Step] Creating publisher factory...");
        var publisherFactory = new PublisherFactory<TestEvent>(schemaRegistryClientFactory);

        await TestContext.Progress.WriteLineAsync("[Publisher Step] Creating publisher...");
        var publisher = publisherFactory.CreatePublisher(publisherOptions);

        await TestContext.Progress.WriteLineAsync("[Publisher Step] Connecting to broker...");
        await publisher.CreateConnection();
        await TestContext.Progress.WriteLineAsync("[Publisher Step] Publisher connected!");

        _context.Publisher = publisher;
        _context.Topic = topic;
    }

    [AfterScenario]
    public async Task Cleanup()
    {
        await TestContext.Progress.WriteLineAsync("[Cleanup] Starting publisher cleanup...");

        if (_context.TryGetPublisher(out var publisher)
            && publisher is IAsyncDisposable publisherDisposable)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(CleanupTimeoutSeconds));
            try
            {
                var disposeTask = publisherDisposable.DisposeAsync().AsTask();
                var completedTask = await Task.WhenAny(disposeTask, Task.Delay(Timeout.Infinite, cts.Token));

                if (completedTask != disposeTask)
                {
                    await TestContext.Progress.WriteLineAsync(
                        $"[Cleanup] WARNING: Publisher dispose timed out after {CleanupTimeoutSeconds}s");
                }
            }
            catch (OperationCanceledException)
            {
                await TestContext.Progress.WriteLineAsync("[Cleanup] Publisher dispose was cancelled");
            }
            catch (Exception ex)
            {
                await TestContext.Progress.WriteLineAsync($"[Cleanup] Error disposing publisher: {ex.Message}");
            }
        }

        await TestContext.Progress.WriteLineAsync("[Cleanup] Publisher cleanup complete");
    }
}
