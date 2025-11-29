using System.Threading.Channels;
using BddE2eTests.Configuration;
using BddE2eTests.Configuration.Builder;
using Publisher.Domain.Port;
using Reqnroll;
using Subscriber.Domain;

namespace BddE2eTests.Steps;

public class TestContext(ScenarioContext scenarioContext)
{
    private const string PublisherKey = "Publisher";
    private const string SubscriberKey = "Subscriber";
    private const string ReceivedMessagesKey = "ReceivedMessages";
    private const string TopicKey = "Topic";
    private const string SentMessageKey = "SentMessage";
    private const string PublisherOptionsBuilderKey = "PublisherOptionsBuilder";
    private const string SubscriberOptionsBuilderKey = "SubscriberOptionsBuilder";

    public IPublisher Publisher
    {
        get => scenarioContext.Get<IPublisher>(PublisherKey);
        set => scenarioContext.Set(value, PublisherKey);
    }

    public ISubscriber Subscriber
    {
        get => scenarioContext.Get<ISubscriber>(SubscriberKey);
        set => scenarioContext.Set(value, SubscriberKey);
    }

    public Channel<string> ReceivedMessages
    {
        get => scenarioContext.Get<Channel<string>>(ReceivedMessagesKey);
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

    public bool TryGetPublisher(out IPublisher? publisher)
    {
        return scenarioContext.TryGetValue(PublisherKey, out publisher);
    }

    public bool TryGetSubscriber(out ISubscriber? subscriber)
    {
        return scenarioContext.TryGetValue(SubscriberKey, out subscriber);
    }

    public bool TryGetReceivedMessages(out Channel<string>? channel)
    {
        return scenarioContext.TryGetValue(ReceivedMessagesKey, out channel);
    }

    public PublisherOptionsBuilder GetOrCreatePublisherOptionsBuilder()
    {
        if (!scenarioContext.TryGetValue(PublisherOptionsBuilderKey, out PublisherOptionsBuilder builder))
        {
            builder = new PublisherOptionsBuilder();
            scenarioContext.Set(builder, PublisherOptionsBuilderKey);
        }
        return builder;
    }

    public SubscriberOptionsBuilder GetOrCreateSubscriberOptionsBuilder()
    {
        if (!scenarioContext.TryGetValue(SubscriberOptionsBuilderKey, out SubscriberOptionsBuilder builder))
        {
            builder = new SubscriberOptionsBuilder();
            scenarioContext.Set(builder, SubscriberOptionsBuilderKey);
        }
        return builder;
    }
}

