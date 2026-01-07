using System.Collections.Generic;
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
    private const string PublishersKey = "Publishers";
    private const string SubscribersKey = "Subscribers";
    private const string ReceivedMessagesKey = "ReceivedMessages";
    private const string SubscriberReceivedMessagesKey = "SubscriberReceivedMessages";
    private const string TopicKey = "Topic";
    private const string SentMessageKey = "SentMessage";
    private const string PublisherOptionsBuilderKey = "PublisherOptionsBuilder";
    private const string SubscriberOptionsBuilderKey = "SubscriberOptionsBuilder";
    private const string SchemaRegistryClientBuilderKey = "SchemaRegistryClientBuilder";
    private const string PublishExceptionKey = "PublishException";
    private const string CommittedOffsetKey = "CommittedOffset";

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

    public ulong? CommittedOffset
    {
        get => scenarioContext.TryGetValue(CommittedOffsetKey, out ulong? offset) ? offset : null;
        set => scenarioContext.Set(value, CommittedOffsetKey);
    }

    private Dictionary<string, IPublisher<TestEvent>> Publishers
    {
        get
        {
            if (!scenarioContext.TryGetValue(PublishersKey, out Dictionary<string, IPublisher<TestEvent>>? publishers))
            {
                publishers = new Dictionary<string, IPublisher<TestEvent>>();
                scenarioContext.Set(publishers, PublishersKey);
            }
            return publishers;
        }
    }

    public void SetPublisher(string name, IPublisher<TestEvent> publisher)
    {
        Publishers[name] = publisher;
    }

    public IPublisher<TestEvent> GetPublisher(string name)
    {
        if (!Publishers.TryGetValue(name, out var publisher))
        {
            throw new KeyNotFoundException($"Publisher '{name}' not found. Available publishers: {string.Join(", ", Publishers.Keys)}");
        }
        return publisher;
    }

    public IEnumerable<IPublisher<TestEvent>> GetAllPublishers()
    {
        return Publishers.Values;
    }

    private Dictionary<string, ISubscriber<TestEvent>> Subscribers
    {
        get
        {
            if (!scenarioContext.TryGetValue(SubscribersKey, out Dictionary<string, ISubscriber<TestEvent>>? subscribers))
            {
                subscribers = new Dictionary<string, ISubscriber<TestEvent>>();
                scenarioContext.Set(subscribers, SubscribersKey);
            }
            return subscribers;
        }
    }

    private Dictionary<string, Channel<TestEvent>> SubscriberReceivedMessages
    {
        get
        {
            if (!scenarioContext.TryGetValue(SubscriberReceivedMessagesKey, out Dictionary<string, Channel<TestEvent>>? messages))
            {
                messages = new Dictionary<string, Channel<TestEvent>>();
                scenarioContext.Set(messages, SubscriberReceivedMessagesKey);
            }
            return messages;
        }
    }

    public void SetSubscriber(string name, ISubscriber<TestEvent> subscriber, Channel<TestEvent> receivedMessages)
    {
        Subscribers[name] = subscriber;
        SubscriberReceivedMessages[name] = receivedMessages;
    }

    public ISubscriber<TestEvent> GetSubscriber(string name)
    {
        if (!Subscribers.TryGetValue(name, out var subscriber))
        {
            throw new KeyNotFoundException($"Subscriber '{name}' not found. Available subscribers: {string.Join(", ", Subscribers.Keys)}");
        }
        return subscriber;
    }

    public Channel<TestEvent> GetSubscriberReceivedMessages(string name)
    {
        if (!SubscriberReceivedMessages.TryGetValue(name, out var messages))
        {
            throw new KeyNotFoundException($"Received messages channel for subscriber '{name}' not found. Available subscribers: {string.Join(", ", SubscriberReceivedMessages.Keys)}");
        }
        return messages;
    }

    public IEnumerable<ISubscriber<TestEvent>> GetAllSubscribers()
    {
        return Subscribers.Values;
    }
}
