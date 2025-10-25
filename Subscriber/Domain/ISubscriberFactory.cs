using Subscriber.Configuration;

namespace Subscriber.Domain;

public interface ISubscriberFactory
{
    ISubscriber CreateSubscriber(SubscriberOptions options, Func<string, Task>? messageHandler);
}