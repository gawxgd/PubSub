using Subscriber.Configuration;
using Subscriber.Configuration.Options;

namespace Subscriber.Domain;

public interface ISubscriberFactory<T>
{
    ISubscriber<T> CreateSubscriber(SubscriberOptions options, Func<T, Task>? messageHandler);
}