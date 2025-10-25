using Subscriber.Configuration;

namespace Subscriber.Domain;

public interface ISubscriberFactory
{
    ISubscriber CreateSubscriber(SubscriberOptions options);
}