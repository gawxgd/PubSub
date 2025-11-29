using Reqnroll;
using Publisher.Configuration;
using BddE2eTests.Configuration;

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

    private readonly ScenarioTestContext _context = new(scenarioContext);

    [Given(@"a publisher is configured with the following options:")]
    public async Task GivenAPublisherIsConfiguredWithTheFollowingOptions(Table table)
    {
        var builder = _context.GetOrCreatePublisherOptionsBuilder();
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

        var publisherOptions = builder.Build();
        var publisherFactory = new PublisherFactory();
        var publisher = publisherFactory.CreatePublisher(publisherOptions);
        await publisher.CreateConnection();

        _context.Publisher = publisher;
        _context.Topic = topic;
    }
}

