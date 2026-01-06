using System.Threading.Channels;
using BddE2eTests.Configuration.Builder;
using BddE2eTests.Configuration.Options;
using BddE2eTests.Configuration.TestEvents;
using Publisher.Domain.Port;
using Reqnroll;
using Subscriber.Domain;

namespace BddE2eTests.Configuration;

public class ScenarioTestContext(ScenarioContext scenarioContext)
{
    private const string PublisherKey = "Publisher";
    private const string SubscriberKey = "Subscriber";
    private const string ReceivedMessagesKey = "ReceivedMessages";
    private const string TopicKey = "Topic";
    private const string SentMessageKey = "SentMessage";
    private const string PublisherOptionsBuilderKey = "PublisherOptionsBuilder";
    private const string SubscriberOptionsBuilderKey = "SubscriberOptionsBuilder";
    private const string SchemaRegistryClientBuilderKey = "SchemaRegistryClientBuilder";
    private const string PublishExceptionKey = "PublishException";

    private static readonly TestOptions TestOptions = TestOptionsLoader.Load();
    
    public SchemaRegistryTestOptions SchemaRegistryOptions => TestOptions.SchemaRegistry;

    public IPublisher<TestEvent> Publisher
    {
        get => scenarioContext.Get<IPublisher<TestEvent>>(PublisherKey);
        set => scenarioContext.Set(value, PublisherKey);
    }

    public ISubscriber<TestEvent> Subscriber
    {
        get => scenarioContext.Get<ISubscriber<TestEvent>>(SubscriberKey);
        set => scenarioContext.Set(value, SubscriberKey);
    }

    public Channel<TestEvent> ReceivedMessages
    {
        get => scenarioContext.Get<Channel<TestEvent>>(ReceivedMessagesKey);
        set => scenarioContext.Set(value, ReceivedMessagesKey);
    }

    public string Topic
    {
        get => scenarioContext.Get<string>(TopicKey);
        set => scenarioContext.Set(value, TopicKey);
    }

    public string SentMessage
    {
        get => scenarioContext.Get<string>(SentMessageKey);
        set => scenarioContext.Set(value, SentMessageKey);
    }

    public bool TryGetPublisher(out IPublisher<TestEvent>? publisher)
    {
        return scenarioContext.TryGetValue(PublisherKey, out publisher);
    }

    public bool TryGetSubscriber(out ISubscriber<TestEvent>? subscriber)
    {
        return scenarioContext.TryGetValue(SubscriberKey, out subscriber);
    }

    public bool TryGetReceivedMessages(out Channel<TestEvent>? channel)
    {
        return scenarioContext.TryGetValue(ReceivedMessagesKey, out channel);
    }

    public PublisherOptionsBuilder GetOrCreatePublisherOptionsBuilder()
    {
        if (!scenarioContext.TryGetValue(PublisherOptionsBuilderKey, out PublisherOptionsBuilder builder))
        {
            builder = new PublisherOptionsBuilder(TestOptions.Publisher);
            scenarioContext.Set(builder, PublisherOptionsBuilderKey);
        }

        return builder;
    }

    public SubscriberOptionsBuilder GetOrCreateSubscriberOptionsBuilder()
    {
        if (!scenarioContext.TryGetValue(SubscriberOptionsBuilderKey, out SubscriberOptionsBuilder builder))
        {
            builder = new SubscriberOptionsBuilder(TestOptions.Subscriber);
            scenarioContext.Set(builder, SubscriberOptionsBuilderKey);
        }

        return builder;
    }

    public SchemaRegistryClientBuilder GetOrCreateSchemaRegistryClientBuilder()
    {
        if (!scenarioContext.TryGetValue(SchemaRegistryClientBuilderKey, out SchemaRegistryClientBuilder builder))
        {
            builder = new SchemaRegistryClientBuilder(TestOptions.SchemaRegistry);
            scenarioContext.Set(builder, SchemaRegistryClientBuilderKey);
        }

        return builder;
    }

    public Exception? PublishException
    {
        get => scenarioContext.TryGetValue(PublishExceptionKey, out Exception? ex) ? ex : null;
        set => scenarioContext.Set(value, PublishExceptionKey);
    }
}
