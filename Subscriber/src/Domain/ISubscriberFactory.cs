using Subscriber.Configuration.Options;

namespace Subscriber.Domain;

public interface ISubscriberFactory<T> where T : new()
{
    ISubscriber<T> CreateSubscriber(SubscriberOptions options, Func<T, Task> messageHandler);
}
